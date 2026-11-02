# kafka-order-events

Демонстрационный проект из трёх модулей: два микросервиса на **Spring Boot 3.5 / Java 21 / Gradle**
с **PostgreSQL**, которые обмениваются сообщениями через **Kafka в Docker (KRaft, без ZooKeeper)**.

```
                    ┌──────────────────────────┐
   POST /api/orders │  producer-service  :8080 │
 ─────────────────▶ │                          │
                    │  1. валидация            │
                    │  2. INSERT в orders      │──── producer_db :5433
                    │  3. publish (sync)       │
                    └───────────┬──────────────┘
                                │  topic: orders.created (key = orderId)
                                ▼
                    ┌──────────────────────────┐
                    │   Kafka 3.9 (KRaft)      │ :9092 (host) / :29092 (docker)
                    └───────────┬──────────────┘
                                │  @KafkaListener, groupId=consumer-service
                                ▼
                    ┌──────────────────────────┐
   GET /api/         │  consumer-service :8081  │
   processed-orders  │                          │
 ◀────────────────── │  4. INSERT ... ON        │──── consumer_db :5434
                     │     CONFLICT DO NOTHING  │
                     └──────────────────────────┘
```

## Модули

| Модуль             | Что внутри                                                                    |
|--------------------|-------------------------------------------------------------------------------|
| `order-contract`   | Общий контракт: имена топиков (`KafkaTopics`) и событие `OrderCreatedEvent` (record). Оба сервиса зависят только от него, поэтому формат сообщения не может разойтись. |
| `producer-service` | REST API создания/чтения заказов, JPA + Flyway, публикация события в Kafka.    |
| `consumer-service` | Kafka-листенер, идемпотентная запись в свою БД, read-API, DLT для «плохих» сообщений. |
| `docker-infra`     | `docker-compose.yml`: Kafka (KRaft), Kafka UI, два PostgreSQL, + Dockerfile для сервисов. |
| `tools/e2e`        | E2E-тесты на pytest: поднимают стек и проверяют весь путь запроса.              |

Сборка описана на **Kotlin DSL** (`build.gradle.kts`, `settings.gradle.kts`): скрипты типизированы и
проверяются компилятором при конфигурации, а версии по-прежнему сосредоточены в
`gradle/libs.versions.toml`. Расширение `integrationTest` объявлено в корневом скрипте и доступно всем
модулям.

## Порты

| Что                     | Порт |
|-------------------------|------|
| producer-service        | 8080 |
| consumer-service        | 8081 |
| Kafka (listener для хоста) | 9092 |
| Kafka UI                | 8090 |
| PostgreSQL producer_db  | 5433 |
| PostgreSQL consumer_db  | 5434 |

Порты 5433/5434 выбраны специально, чтобы не конфликтовать с локально установленным PostgreSQL на 5432.

## Быстрый старт

Требуется: JDK 21, Docker + Docker Compose.

> **macOS:** пошаговая инструкция с учётом Homebrew, Apple Silicon и типовых ошибок —
> в разделе [Локальный запуск на macOS](#локальный-запуск-на-macos).

```bash
# 1. Инфраструктура (Kafka + Kafka UI + 2 x PostgreSQL)
docker compose -f docker-infra/docker-compose.yml up -d

# 2. Сервисы (каждый в своём терминале)
./gradlew :producer-service:bootRun
./gradlew :consumer-service:bootRun
```

Создать заказ:

```bash
curl -s -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","product":"Mechanical keyboard","quantity":2,"totalPrice":259.98,"currency":"EUR"}'
```

Ответ содержит `id` заказа и `eventId` события:

```json
{
  "id": "4b1c0b4e-...", "eventId": "d0f0f0a1-...",
  "customerId": "customer-1", "product": "Mechanical keyboard",
  "quantity": 2, "totalPrice": 259.98, "currency": "EUR",
  "status": "PUBLISHED", "createdAt": "2024-05-01T10:15:30Z", "publishedAt": "2024-05-01T10:15:30Z"
}
```

Убедиться, что событие дошло до второго сервиса:

```bash
curl -s http://localhost:8081/api/processed-orders/by-event/<eventId>
curl -s http://localhost:8081/api/processed-orders/stats
```

Тот же заказ в Kafka UI: <http://localhost:8090> → кластер `local` → топик `orders.created`.

### Всё в Docker

```bash
docker compose -f docker-infra/docker-compose.yml --profile full up -d --build
```

## Локальный запуск на macOS

Инструкция проверена на **macOS (Apple Silicon, arm64)** с Docker 29.x и Gradle 8.10.2.
Все команды выполняются из корня репозитория (там, где лежит `gradlew`).

### 0. Что понадобится

| Компонент | Версия | Зачем |
|-----------|--------|-------|
| JDK | **21** (не 17 и не 23) | Проект собирается Gradle toolchain'ом `languageVersion = 21`; см. `build.gradle.kts` |
| Docker Desktop (или colima) | Compose v2 | Kafka, Kafka UI, два PostgreSQL |
| Xcode Command Line Tools | любая актуальная | `git`, `python3`, `curl` уже включены в macOS |

Проверить, что уже есть:

```bash
git --version
python3 --version        # в macOS поставляется Python 3.9 — этого достаточно для tools/e2e
docker compose version    # нужен именно "Docker Compose version v2.x", а не старый docker-compose v1
```

### 1. JDK 21

Важная деталь: в `gradle.properties` стоит `org.gradle.java.installations.auto-download=false` —
Gradle **не будет** скачивать JDK сам. Если JDK 21 в системе нет, сборка сразу упадёт с ошибкой
(см. [раздел про проблемы](#типичные-проблемы-на-macos)). Сама JVM, на которой стартует Gradle, при этом
может быть любой — toolchain найдёт 21 отдельно, лишь бы он был установлен.

Самый простой способ — cask: он регистрирует JDK в `/Library/Java/JavaVirtualMachines`, и его видят
и `/usr/libexec/java_home`, и Gradle:

```bash
brew install --cask temurin@21        # либо: brew install --cask corretto@21
```

Прописать `JAVA_HOME` в `~/.zshrc` (macOS с 10.15 использует zsh):

```bash
# ~/.zshrc
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

Применить и проверить:

```bash
source ~/.zshrc
/usr/libexec/java_home -V     # в списке должен быть пункт 21.x
java -version                 # openjdk version "21.x"
```

> **Альтернатива — `brew install openjdk@21`.** Эта формула **keg-only**: она не попадает в
> `/Library/Java/JavaVirtualMachines`, поэтому `/usr/libexec/java_home -v 21` её не найдёт и Gradle
> тоже не увидит при авто-детекте. Если вы всё же выбрали её, укажите путь явно — любой из вариантов:
>
> ```bash
> # вариант A: JAVA_HOME на конкретный запуск/шелл
> export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
>
> # вариант B: разово передать путь Gradle (Intel: /usr/local/opt/...)
> ./gradlew build -Porg.gradle.java.installations.paths=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
> ```

Убедиться, что Gradle видит 21:

```bash
./gradlew -q javaToolchains
```

В выводе должна быть строка `Language Version: 21` и `Auto-download: Disabled`.

### 2. Docker

1. Установить Docker Desktop (или `brew install colima docker docker-compose && colima start`).
2. Дождаться статуса **Engine running** — пока демон не запущен, любая команда `docker` вернёт ошибку.
3. Выделить движку хотя бы **4 GB RAM**: внутри работают JVM Kafka и два инстанса PostgreSQL
   (Docker Desktop → Settings → Resources).

Проверка:

```bash
docker info --format '{{.ServerVersion}}'
docker compose version
```

Образы `apache/kafka:3.9.2`, `provectuslabs/kafka-ui:v0.7.2` и `postgres:16-alpine` имеют нативные
сборки под **arm64**, поэтому на Apple Silicon Rosetta и `platform:` не нужны.

### 3. Клонирование и подготовка

```bash
git clone <repo-url> kafka-order-events
cd kafka-order-events

# Если проект скачан архивом или скопирован через Finder, бит исполнения у gradlew может потеряться:
chmod +x gradlew

# Снимаем карантин macOS, если он появился после распаковки архива:
xattr -d com.apple.quarantine gradlew 2>/dev/null || true
```

Порты по умолчанию: `8080`, `8081`, `9092`, `8090`, `5433`, `5434`. Проверить, что они свободны:

```bash
for p in 8080 8081 9092 8090 5433 5434; do
  printf '%s: ' "$p"
  lsof -nP -iTCP:$p -sTCP:LISTEN 2>/dev/null | tail -n +2 | awk '{print $1}' | paste -sd, - | sed 's/^$/free/'
done
```

Если порт занят, его можно переопределить. Скопируйте `docker-infra/.env.example` в
`docker-infra/.env` и поменяйте значения — Compose подхватывает `.env` из каталога compose-файла:

```bash
cp docker-infra/.env.example docker-infra/.env
```

### 4. Инфраструктура

```bash
docker compose -f docker-infra/docker-compose.yml up -d

# ждём статус healthy у kafka-broker, producer-db, consumer-db (~10–30 секунд)
docker compose -f docker-infra/docker-compose.yml ps
```

Ожидаемый вывод:

```
NAME           STATUS
consumer-db    Up (healthy)
kafka-broker   Up (healthy)
kafka-ui       Up
producer-db    Up (healthy)
```

`kafka-ui` намеренно без healthcheck — он поднимается последним и ему нужно несколько секунд.
Пока Kafka не `healthy`, не запускайте сервисы: они получат `Connection to node -1 could not be established`.

### 5. Запуск сервисов

Каждый сервис — в своём терминале (оба процесса должны работать одновременно).
В каждом новом окне терминала `JAVA_HOME` должен быть выставлен — проверьте `echo $JAVA_HOME`.

```bash
# терминал 1
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew :producer-service:bootRun

# терминал 2
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew :consumer-service:bootRun
```

Готовность видна по строке `Started ProducerServiceApplication` / `Started ConsumerServiceApplication`.
Первая сборка скачивает зависимости, поэтому занимает несколько минут; повторные запуски — секунды.

Проверка health (включает БД и Kafka):

```bash
curl -s http://localhost:8080/actuator/health | python3 -m json.tool
curl -s http://localhost:8081/actuator/health | python3 -m json.tool
```

Оба должны вернуть `"status": "UP"`.

### 6. Сквозная проверка

```bash
RESP=$(curl -s -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-1","product":"Mechanical keyboard","quantity":2,"totalPrice":259.98,"currency":"EUR"}')

echo "$RESP" | python3 -m json.tool

EVENT_ID=$(echo "$RESP" | python3 -c 'import json,sys; print(json.load(sys.stdin)["eventId"])')

# consumer обрабатывает событие асинхронно: первый запрос может вернуть 404,
# повторяйте, пока не появится 200
curl -s "http://localhost:8081/api/processed-orders/by-event/$EVENT_ID" | python3 -m json.tool

curl -s http://localhost:8081/api/processed-orders/stats | python3 -m json.tool
```

Ожидаемый результат: у заказа `"status": "PUBLISHED"`, проекция во втором сервисе содержит те же
`orderId`/`eventId`, а `stats` показывает `"CREATED": 1`.

Kafka UI: <http://localhost:8090> → кластер `local` → топик `orders.created`.

### 7. Остановка

```bash
# Ctrl+C в терминалах с bootRun, затем:
docker compose -f docker-infra/docker-compose.yml down       # сохранить данные в volume'ах
docker compose -f docker-infra/docker-compose.yml down -v    # удалить БД и данные Kafka
```

Если `bootRun` завершился нештатно и порт остался занят:

```bash
lsof -tiTCP:8080 -sTCP:LISTEN | xargs kill     # при необходимости -9
```

Полный сброс состояния (полезно после смены миграций):

```bash
docker compose -f docker-infra/docker-compose.yml down -v
./gradlew --stop
```

### 8. Тесты на macOS

```bash
# юнит- и slice-тесты — Docker не нужен
./gradlew test

# + интеграционные на Testcontainers (Docker должен быть запущен,
# иначе шаг integrationTest просто пропускается)
./gradlew build

# E2E: сам поднимает compose, собирает jar'ы и стартует оба сервиса на хосте
python3 -m venv tools/e2e/.venv
tools/e2e/.venv/bin/python -m pip install -r tools/e2e/requirements.txt
tools/e2e/.venv/bin/python -m pytest tools/e2e -v
```

Перед E2E обязательно остановите `bootRun` и стек из шага 4 — тест занимает те же порты
и падает с `port 8080 is already in use`, если что-то уже слушает. Стек внутри теста поднимается
и гасится сам; чтобы оставить его для отладки, запускайте с `E2E_KEEP_STACK=1`.

### Типичные проблемы на macOS

| Симптом | Причина | Решение |
|---------|---------|---------|
| `Cannot find a Java installation ... {languageVersion=21 ...}` + `No locally installed toolchains match and toolchain auto-provisioning is not enabled` | Нет JDK 21, а авто-скачивание отключено в `gradle.properties` | `brew install --cask temurin@21`, затем `export JAVA_HOME=$(/usr/libexec/java_home -v 21)` |
| `java -version` показывает 23 или 17 | В `JAVA_HOME`/`PATH` стоит другая JVM по умолчанию | Это не ошибка: Gradle всё равно возьмёт toolchain 21, если он установлен. Проверьте `./gradlew -q javaToolchains` |
| `openjdk@21` установлен, но Gradle его не видит | Формула keg-only, нет регистрации в системе | `export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home` или `-Porg.gradle.java.installations.paths=...` |
| `zsh: permission denied: ./gradlew` | Потерян бит исполнения после архива/Finder | `chmod +x gradlew` |
| `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Kafka ещё не готова, либо сервис запущен в контейнере с адресом хоста | Дождаться `healthy` у `kafka-broker`; в Docker использовать `kafka:29092`, на хосте — `localhost:9092` |
| `Schema-validation: wrong column type ...` / падение Flyway | В volume'ах осталась старая схема | `docker compose ... down -v` и запустить заново |
| `port is already allocated` / `bind: address already in use` | Порт занят локальным PostgreSQL или другим проектом | Переопределить порты в `docker-infra/.env` или освободить порт |
| `Cannot connect to the Docker daemon` | Docker Desktop не запущен | Запустить Docker Desktop / `colima start` |
| Контейнеры периодически перезапускаются, в логах OOM | Мало памяти у Docker VM | Выделить ≥ 4 GB в Docker Desktop → Resources |
| `error: externally-managed-environment` при `pip install` | Системный Python защищён PEP 668 | Использовать venv, как в разделе [Тесты](#8-тесты-на-macos) |
| E2E: `port 8080 is already in use` | Запущены `bootRun` или compose-стек | Остановить их перед запуском тестов |
| `bash: setsid: command not found` | На macOS нет `setsid` | Запускать сервисы в отдельных окнах терминала (или через `nohup ... &`) |

## API

### producer-service

| Метод | Путь                             | Назначение                              |
|-------|----------------------------------|-----------------------------------------|
| POST  | `/api/orders`                    | создать заказ и опубликовать событие    |
| GET   | `/api/orders/{id}`              | заказ по id                             |
| GET   | `/api/orders?customerId=&limit=` | список заказов                          |
| GET   | `/api/orders/by-event/{eventId}` | заказ, породивший событие               |
| GET   | `/actuator/health`               | health (включая Kafka и БД)             |

### consumer-service

| Метод | Путь                                    | Назначение                       |
|-------|-----------------------------------------|----------------------------------|
| GET   | `/api/processed-orders/by-event/{eventId}` | проекция по `eventId` (404, пока не доехало) |
| GET   | `/api/processed-orders/{orderId}`       | проекция по id заказа            |
| GET   | `/api/processed-orders?customerId=&limit=` | список                       |
| GET   | `/api/processed-orders/stats`           | счётчики `CREATED` / `DUPLICATE_IGNORED` / `FAILED` |
| GET   | `/actuator/health`                      | health                           |

## Тесты

```bash
# Юнит- и slice-тесты (не требуют Docker, PostgreSQL и Kafka)
./gradlew test

# + интеграционные тесты на Testcontainers (нужен Docker; без Docker шаг пропускается)
./gradlew build

# Только интеграционные
./gradlew :producer-service:integrationTest :consumer-service:integrationTest

# E2E: поднимает compose, запускает оба сервиса, проверяет весь путь
python3 -m venv tools/e2e/.venv
tools/e2e/.venv/bin/pip install -r tools/e2e/requirements.txt
tools/e2e/.venv/bin/python -m pytest tools/e2e -v
```

Что покрыто:

* **контракт** — сериализация/десериализация `OrderCreatedEvent`, устойчивость к лишним полям, валидация инвариантов;
* **producer** — порядок «сначала в БД, потом в Kafka», деградация при недоступном брокере, релей повторной публикации, ключ записи = `orderId`, 400/404 ответы API;
* **consumer** — идемпотентность при повторной доставке, отсутствие ack при ошибке БД, «плохие» записи уходят в DLT и не блокируют партицию;
* **интеграционные** — Flyway + Hibernate `validate` совпадают с сущностями, реальный round-trip через брокер, дубликаты не создают вторую строку;
* **E2E** — REST → Kafka → второй сервис → БД, два заказа = два разных события, конфигурация compose (KRaft, два listener'а, две БД).

## Принятые решения (и почему)

**KRaft вместо ZooKeeper.** Kafka 3.9 поддерживает KRaft как основной режим: на один контейнер меньше,
и `docker-compose.yml` проще.

**Два listener'а.** `INTERNAL://kafka:29092` — для контейнеров, `EXTERNAL://localhost:9092` — для хоста.
Сервис, запущенный в Docker, должен использовать `kafka:29092`; сервис на хосте — `localhost:9092`.
Перепутать их — самая частая ошибка, поэтому оба адреса заданы явно и покрыты тестом.

**Один топик, ключ = `orderId`.** Все события одного заказа попадают в одну партицию и сохраняют порядок.

**JSON без type headers.** `spring.json.add.type.headers=false`, а десериализатор привязан к
`OrderCreatedEvent`. Это убирает привязку контракта к именам Java-классов и запрещает инстанцировать
произвольные типы из заголовков.

**Идемпотентность на стороне БД, а не приложения.** `INSERT ... ON CONFLICT (order_id) DO NOTHING`
решает «новое или дубликат» одной атомарной операцией. Проверка «select → insert» гоняется при
`concurrency: 3`, а ловля `DataIntegrityViolationException` помечает транзакцию rollback-only.

**Публикация синхронная + статус заказа.** Заказ сначала коммитится (`PUBLISH_FAILED`), затем
публикуется событие, затем статус меняется на `PUBLISHED`. Это осознанный компромисс вместо
transactional outbox: если брокер недоступен, заказ не теряется, а фоновый `OrderEventRelay`
переопубликует событие с **тем же** `eventId` (поэтому дедупликация у потребителя продолжает работать).
`eventId` хранится в БД заказа именно для этого. Полноценный outbox — следующий шаг, если нужна
гарантия «ровно один раз» и строгая атомарность.

**Ошибки в consumer.** Транзиентные (например, БД недоступна) — ретраи с экспоненциальным backoff,
затем DLT `orders.created.DLT`. Невалидные/недесериализуемые — сразу DLT через
`ErrorHandlingDeserializer`. Ничего не «проглатывается»: молчаливый `ack` означал бы потерю события.

**`ddl-auto: validate` + Flyway.** Схемой владеет миграция, Hibernate только проверяет соответствие.
Именно эта настройка поймала реальный баг: `CHAR(3)` в миграции против `varchar(3)` в сущности
(PostgreSQL отдаёт для `CHAR(n)` тип `bpchar`) — исправлено на `VARCHAR(3)` + `CHECK`.

## Известные ограничения

* Нет аутентификации и авторизации — это учебный проект.
* `order-contract` не использует Schema Registry/Avro; для продакшена стоит
  Confluent Schema Registry или protobuf с проверкой совместимости.
* Топики создаются приложением (`NewTopic`) для удобства; в реальной эксплуатации ими управляет платформа.
* `OrderEventRelay` крутится в каждом инстансе producer'а: дубликаты возможны, но безопасны благодаря
  идемпотентности потребителя. Для строгой дисциплины нужен outbox + блокировка по строкам.

## Структура

```
.
├── build.gradle.kts             # общий build: toolchain 21, BOM Spring Boot, integrationTest source set
├── settings.gradle.kts          # 4 модуля
├── gradle/libs.versions.toml    # единая точка версий
├── order-contract/              # KafkaTopics + OrderCreatedEvent
├── producer-service/            # REST + JPA + Kafka producer
├── consumer-service/            # Kafka listener + идемпотентная запись + read-API
├── docker-infra/                # docker-compose.yml, Dockerfile, .env.example
└── tools/e2e/                   # pytest E2E
```
