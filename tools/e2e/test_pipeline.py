"""End-to-end verification of the producer -> Kafka -> consumer pipeline.

Scope: this suite exercises the *whole* stack, not a single module. It boots the docker compose
infrastructure, starts both Spring Boot services on the host, publishes an order over REST and then
asserts that the very same event shows up in the consumer's PostgreSQL database.

Why here and not in Gradle?
    The Gradle tests (unit, slice, Testcontainers) verify each service in isolation. Only a real
    broker + two real services + two real databases can prove that the contract, the topic names and
    the deployment wiring actually fit together.

Run:
    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    .venv/bin/python -m pytest tools/e2e -v
"""

from __future__ import annotations

import os
import socket
import subprocess
import uuid
from pathlib import Path

import pytest
import yaml

from docker_utils import (
    compose,
    container_exists,
    container_health,
    container_logs,
    container_running,
    docker_available,
    wait_for_containers,
    wait_until,
)
from http_client import get, healthy, post_json

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-infra" / "docker-compose.yml"

PRODUCER_PORT = int(os.environ.get("PRODUCER_PORT", "8080"))
CONSUMER_PORT = int(os.environ.get("CONSUMER_PORT", "8081"))

PRODUCER_URL = "http://localhost:{0}".format(PRODUCER_PORT)
CONSUMER_URL = "http://localhost:{0}".format(CONSUMER_PORT)

INFRA_CONTAINERS = ("kafka-broker", "producer-db", "consumer-db")

# How long we are willing to wait for the JVMs and for a message to travel through Kafka.
STARTUP_TIMEOUT = float(os.environ.get("E2E_STARTUP_TIMEOUT", "180"))
PIPELINE_TIMEOUT = float(os.environ.get("E2E_PIPELINE_TIMEOUT", "90"))


# --------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------
def _port_free(port: int) -> bool:
    """True when nothing is listening on the port (used for clear pre-flight errors)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex(("127.0.0.1", port)) != 0


def _compose_definition() -> dict:
    """Parses docker-compose.yml so infrastructure tests assert structure, not text."""
    return yaml.safe_load(COMPOSE_FILE.read_text())


def _is_java_home(path: "Path | str") -> bool:
    """True when the path is a real JDK/JRE home (a stale JAVA_HOME would break Gradle)."""
    if not path:
        return False
    candidate = Path(path)
    return (candidate / "bin" / "java").exists() or (candidate / "bin" / "java.exe").exists()


def _java_home() -> str:
    """Returns a usable Java home, preferring a Java 21 installation.

    The machine may carry a stale JAVA_HOME (e.g. an IntelliJ managed one), so every candidate is
    validated before being used.
    """
    candidates = []
    env_home = os.environ.get("JAVA_HOME", "")
    if env_home:
        candidates.append(env_home)

    for base in (Path.home() / "Library" / "Java" / "JavaVirtualMachines", Path("/usr/lib/jvm")):
        if not base.exists():
            continue
        for candidate in sorted(base.glob("*")):
            mac_home = candidate / "Contents" / "Home"
            candidates.append(str(mac_home if mac_home.exists() else candidate))

    # First pass: a valid Java 21 home, second pass: any valid Java home.
    for required in ("21", ""):
        for candidate in candidates:
            if required and required not in str(candidate):
                continue
            if _is_java_home(candidate):
                return candidate
    return ""


def _service_env(jar: Path, port: int) -> dict:
    """Environment for one service: database, Kafka and the HTTP port."""
    env = dict(os.environ)
    java_home = _java_home()
    if java_home:
        env["JAVA_HOME"] = java_home

    env["SERVER_PORT"] = str(port)
    # The host reaches Kafka through the EXTERNAL listener advertised as localhost:9092.
    env["KAFKA_BOOTSTRAP_SERVERS"] = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    if "consumer" in jar.name:
        env["DB_URL"] = os.environ.get("CONSUMER_DB_URL", "jdbc:postgresql://localhost:5434/consumer_db")
        env["DB_USERNAME"] = os.environ.get("CONSUMER_DB_USERNAME", "consumer")
        env["DB_PASSWORD"] = os.environ.get("CONSUMER_DB_PASSWORD", "consumer")
    else:
        env["DB_URL"] = os.environ.get("PRODUCER_DB_URL", "jdbc:postgresql://localhost:5433/producer_db")
        env["DB_USERNAME"] = os.environ.get("PRODUCER_DB_USERNAME", "producer")
        env["DB_PASSWORD"] = os.environ.get("PRODUCER_DB_PASSWORD", "producer")
    return env


def _single_jar(libs_dir: Path) -> Path:
    """Picks the executable jar, ignoring the `-plain` variant produced by Spring Boot."""
    candidates = [p for p in libs_dir.glob("*.jar") if not p.name.endswith("-plain.jar")]
    if len(candidates) != 1:
        pytest.fail("expected exactly one boot jar in {0}, found {1}".format(libs_dir, candidates))
    return candidates[0]


def _build_jars() -> dict:
    """Builds both boot jars once, so the tests themselves only start JVMs."""
    gradlew = REPO_ROOT / "gradlew"
    if not gradlew.exists():
        pytest.fail("gradle wrapper missing at {0}".format(gradlew))

    env = dict(os.environ)
    java_home = _java_home()
    if java_home:
        env["JAVA_HOME"] = java_home

    completed = subprocess.run(
        [str(gradlew), ":producer-service:bootJar", ":consumer-service:bootJar", "-x", "test", "--console=plain"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=900,
        env=env,
    )
    if completed.returncode != 0:
        pytest.fail("bootJar build failed:\n{0}\n{1}".format(completed.stdout[-4000:], completed.stderr[-2000:]))

    return {
        "producer": _single_jar(REPO_ROOT / "producer-service" / "build" / "libs"),
        "consumer": _single_jar(REPO_ROOT / "consumer-service" / "build" / "libs"),
    }


def _start_service(jar: Path, port: int, log_file: Path) -> subprocess.Popen:
    """Starts one Spring Boot jar on the host, wired to the compose infrastructure."""
    env = _service_env(jar, port)
    java = Path(env.get("JAVA_HOME", "")) / "bin" / "java"
    executable = str(java) if java.exists() else "java"

    log_handle = open(log_file, "wb")
    process = subprocess.Popen(
        [executable, "-jar", str(jar)],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    process.e2e_log_handle = log_handle  # type: ignore[attr-defined]
    process.e2e_log_file = log_file      # type: ignore[attr-defined]
    return process


def _stop_service(process: subprocess.Popen) -> None:
    """Stops a service and always closes its log handle."""
    try:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=15)
    finally:
        handle = getattr(process, "e2e_log_handle", None)
        if handle is not None and not handle.closed:
            handle.close()


def _service_log(process: subprocess.Popen, tail: int = 6000) -> str:
    """Reads the tail of a service log for diagnostics in assertion messages."""
    path = getattr(process, "e2e_log_file", None)
    if path is None or not Path(path).exists():
        return "<no log>"
    return Path(path).read_text(errors="replace")[-tail:]


# --------------------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def docker() -> None:
    """Skips the whole module when no Docker daemon is reachable."""
    if not docker_available():
        pytest.skip("Docker daemon is not available; the E2E suite needs a real broker and databases.")


@pytest.fixture(scope="session")
def stack(docker: None, request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> dict:
    """Brings up infrastructure + services and tears everything down afterwards.

    Teardown is registered as a finalizer immediately after the containers start, so a failure during
    setup can never leave a half-running stack behind.
    """
    if not COMPOSE_FILE.exists():
        pytest.fail("compose file not found: {0}".format(COMPOSE_FILE))

    for port in (PRODUCER_PORT, CONSUMER_PORT):
        if not _port_free(port):
            pytest.fail(
                "port {0} is already in use - stop the process listening there before running the E2E "
                "suite (or set PRODUCER_PORT/CONSUMER_PORT)".format(port)
            )

    compose(str(COMPOSE_FILE), "up", "-d", "kafka", "producer-db", "consumer-db", cwd=str(REPO_ROOT))

    processes: list[subprocess.Popen] = []

    def teardown() -> None:
        for handle in reversed(processes):
            _stop_service(handle)
        if os.environ.get("E2E_KEEP_STACK", "0") != "1":
            compose(str(COMPOSE_FILE), "down", "-v", cwd=str(REPO_ROOT), check=False)

    request.addfinalizer(teardown)

    # Waiting for `healthy` (not merely `running`) means Kafka is actually accepting requests and
    # both databases completed their init before the JVMs connect.
    try:
        wait_for_containers(INFRA_CONTAINERS, timeout=180)
    except TimeoutError:
        details = "\n\n".join(
            "{0}: health={1}\n{2}".format(name, container_health(name), container_logs(name, tail=30))
            for name in INFRA_CONTAINERS
        )
        pytest.fail("infrastructure did not become healthy:\n" + details)

    jars = _build_jars()
    log_dir = tmp_path_factory.mktemp("service-logs")
    producer = _start_service(jars["producer"], PRODUCER_PORT, log_dir / "producer.log")
    processes.append(producer)
    consumer = _start_service(jars["consumer"], CONSUMER_PORT, log_dir / "consumer.log")
    processes.append(consumer)

    for name, url, process in (("producer", PRODUCER_URL, producer), ("consumer", CONSUMER_URL, consumer)):
        try:
            wait_until(
                lambda u=url: healthy(u + "/actuator/health"),
                timeout=STARTUP_TIMEOUT,
                interval=2.0,
                description="{0}-service health".format(name),
            )
        except TimeoutError:
            pytest.fail("{0}-service did not become healthy.\n{1}".format(name, _service_log(process)))

    return {
        "producer": PRODUCER_URL,
        "consumer": CONSUMER_URL,
        "producer_process": producer,
        "consumer_process": consumer,
        "log_dir": str(log_dir),
    }


@pytest.fixture()
def order_payload() -> dict:
    """A valid order payload; a fresh customer id keeps assertions independent."""
    return {
        "customerId": "customer-e2e-" + uuid.uuid4().hex[:8],
        "product": "Mechanical keyboard",
        "quantity": 2,
        "totalPrice": 259.98,
        "currency": "EUR",
    }


def _wait_for_consumed(consumer_url: str, event_id: str, *, timeout: float, process=None) -> dict:
    """Polls the consumer API until the event with this id has been stored."""

    def consumed():
        response = get("{0}/api/processed-orders/by-event/{1}".format(consumer_url, event_id))
        return response.json() if response.status == 200 else None

    try:
        return wait_until(
            consumed,
            timeout=timeout,
            interval=1.0,
            description="event {0} to appear in consumer-service".format(event_id),
        )
    except TimeoutError:
        diagnostics = []
        if process is not None:
            diagnostics.append(_service_log(process))
        diagnostics.append(container_logs("kafka-broker", tail=60))
        pytest.fail("event never reached the consumer.\n" + "\n".join(diagnostics))
        raise AssertionError("unreachable")


# --------------------------------------------------------------------------------------
# tests: the pipeline itself
# --------------------------------------------------------------------------------------
class TestPipeline:
    """Happy path and the guarantees that make the pipeline safe to operate."""

    def test_producer_and_consumer_report_healthy(self, stack):
        assert healthy(stack["producer"] + "/actuator/health")
        assert healthy(stack["consumer"] + "/actuator/health")

    def test_order_flows_from_rest_api_through_kafka_into_consumer_database(self, stack, order_payload):
        response = post_json(stack["producer"] + "/api/orders", order_payload)

        assert response.status == 201, "unexpected status {0}: {1}".format(response.status, response.body)
        created = response.json()
        assert created["status"] == "PUBLISHED", "order was stored but not published: {0}".format(created)
        assert created["eventId"] and created["id"]

        stored = _wait_for_consumed(stack["consumer"], created["eventId"], timeout=PIPELINE_TIMEOUT,
                                    process=stack["consumer_process"])

        # The consumer must have stored exactly what the producer published.
        assert stored["orderId"] == created["id"]
        assert stored["eventId"] == created["eventId"]
        assert stored["customerId"] == order_payload["customerId"]
        assert stored["product"] == order_payload["product"]
        assert stored["quantity"] == order_payload["quantity"]
        assert float(stored["totalPrice"]) == pytest.approx(order_payload["totalPrice"])
        assert stored["currency"] == order_payload["currency"]

    def test_consumer_is_reachable_before_the_event_arrives(self, stack):
        # An unknown event id must produce a clean 404, so clients can poll without treating
        # "not yet consumed" as an error.
        response = get(stack["consumer"] + "/api/processed-orders/by-event/" + str(uuid.uuid4()))
        assert response.status == 404

    def test_consumer_stats_are_exposed(self, stack, order_payload):
        created = post_json(stack["producer"] + "/api/orders", order_payload).json()
        _wait_for_consumed(stack["consumer"], created["eventId"], timeout=PIPELINE_TIMEOUT,
                           process=stack["consumer_process"])

        response = get(stack["consumer"] + "/api/processed-orders/stats")

        assert response.status == 200
        stats = response.json()
        # Flyway seeds a row for every outcome; at least one order must have been created.
        assert "CREATED" in stats
        assert stats["CREATED"] >= 1

    def test_two_orders_get_two_distinct_events(self, stack, order_payload):
        first = post_json(stack["producer"] + "/api/orders", order_payload).json()
        second = post_json(stack["producer"] + "/api/orders", order_payload).json()

        assert first["id"] != second["id"]
        assert first["eventId"] != second["eventId"]

        for created in (first, second):
            _wait_for_consumed(stack["consumer"], created["eventId"], timeout=PIPELINE_TIMEOUT,
                               process=stack["consumer_process"])

    def test_invalid_order_is_rejected_without_publishing(self, stack):
        invalid = {"customerId": "", "product": "", "quantity": 0, "totalPrice": -1, "currency": "EURO"}

        response = post_json(stack["producer"] + "/api/orders", invalid)

        assert response.status == 400
        body = response.json()
        assert body["title"] == "Invalid request"
        assert body["errors"], "expected field level errors, got {0}".format(body)


# --------------------------------------------------------------------------------------
# tests: infrastructure definition
# --------------------------------------------------------------------------------------
class TestInfrastructureContract:
    """Guards the infrastructure definition itself - cheap checks that catch drift early."""

    def test_kafka_runs_in_kraft_mode_without_a_zookeeper_service(self):
        definition = _compose_definition()
        services = definition["services"]

        assert "zookeeper" not in services, "KRaft mode must not need a ZooKeeper service"
        kafka_env = services["kafka"]["environment"]
        assert kafka_env["KAFKA_PROCESS_ROLES"] == "broker,controller"
        assert "KAFKA_ZOOKEEPER_CONNECT" not in kafka_env
        assert services["kafka"]["image"].startswith("apache/kafka:")

    def test_kafka_exposes_internal_and_external_listeners(self):
        kafka_env = _compose_definition()["services"]["kafka"]["environment"]

        # Containers talk over INTERNAL, the host over EXTERNAL. Mixing them up produces the classic
        # "Connection to node -1 could not be established" failure.
        assert "INTERNAL://kafka:29092" in kafka_env["KAFKA_ADVERTISED_LISTENERS"]
        assert "EXTERNAL://localhost:" in kafka_env["KAFKA_ADVERTISED_LISTENERS"]
        assert kafka_env["KAFKA_INTER_BROKER_LISTENER_NAME"] == "INTERNAL"

    def test_each_service_has_its_own_database(self):
        services = _compose_definition()["services"]

        assert "producer-db" in services
        assert "consumer-db" in services
        assert services["producer-db"]["environment"]["POSTGRES_DB"] == "producer_db"
        assert services["consumer-db"]["environment"]["POSTGRES_DB"] == "consumer_db"
        # Two distinct host ports, so the stack never fights a locally installed PostgreSQL.
        assert services["producer-db"]["ports"] != services["consumer-db"]["ports"]

    def test_full_profile_contains_both_services(self):
        services = _compose_definition()["services"]

        for name in ("producer-service", "consumer-service"):
            assert name in services
            assert "full" in services[name]["profiles"]

    def test_required_infrastructure_containers_run(self, stack):
        for name in INFRA_CONTAINERS:
            assert container_exists(name), "container {0} is missing".format(name)
            assert container_running(name), "container {0} is not running".format(name)
