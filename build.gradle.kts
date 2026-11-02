/*
 * Root build of the "kafka-order-events" sample project.
 *
 * Layout
 *   :order-contract    - shared event contract (topic names + JSON payload)
 *   :producer-service  - Spring Boot REST service: stores orders in Postgres, publishes Kafka events
 *   :consumer-service  - Spring Boot Kafka listener: consumes events into its own Postgres
 *   :docker-infra      - docker-compose stack (Kafka KRaft, Kafka UI, 2x Postgres) + Dockerfiles
 *
 * The root project itself contains no code: it only configures the subprojects.
 */

import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.jvm.toolchain.JavaLanguageVersion
import java.util.concurrent.TimeUnit

plugins {
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
}

// Version catalog accessors are only resolvable on a project that declares the catalog, so the
// Spring Boot version is read once here and reused inside `subprojects { }` below.
val springBootVersion = libs.versions.spring.boot.get()

allprojects {
    group = "com.example.orders"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

/*
 * Docker is only needed by the *integrationTest* source set. Probing once keeps the default
 * `./gradlew build` green on machines without a Docker daemon: the task is skipped with an
 * explicit message instead of failing the build.
 */
fun dockerAvailable(): Boolean {
    return try {
        val process = ProcessBuilder("docker", "info", "--format", "{{.ServerVersion}}")
            .redirectErrorStream(true)
            .start()
        if (!process.waitFor(20, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            return false
        }
        // Drain the output so the process can terminate cleanly on every platform.
        process.inputStream.readBytes()
        process.exitValue() == 0
    } catch (ignored: Exception) {
        false
    }
}

val dockerReady: Boolean = dockerAvailable()

// Kept in `extra` for parity with the previous Groovy build (and for init scripts / CI probes).
extra["dockerReady"] = dockerReady

subprojects {
    apply(plugin = "java")
    apply(plugin = "io.spring.dependency-management")

    /*
     * Every module (including the Spring-free :order-contract) imports the Spring Boot BOM, so
     * Jackson, Flyway, Testcontainers and friends always resolve to the same tested versions.
     * The version is single-sourced in gradle/libs.versions.toml.
     */
    configure<DependencyManagementExtension> {
        imports {
            mavenBom("org.springframework.boot:spring-boot-dependencies:$springBootVersion")
        }
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    /*
     * Boot 3.5 manages a newer JUnit Platform than the launcher Gradle 8.10 injects. Declaring the
     * launcher explicitly (version taken from the BOM) keeps engine and launcher aligned.
     */
    dependencies {
        add("testRuntimeOnly", "org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        // Spring needs parameter names for @RequestParam / @PathVariable / constructor binding.
        options.compilerArgs.add("-parameters")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("failed", "skipped")
            exceptionFormat = TestExceptionFormat.FULL
        }
    }

    /*
     * The `src/integrationTest` tree is a separate source set, so Docker based tests never run during a
     * plain unit test cycle, yet they are still wired into `check` when a Docker daemon exists.
     */
    val sourceSets = extensions.getByType<SourceSetContainer>()
    val mainSourceSet = sourceSets.named("main")
    val testSourceSet = sourceSets.named("test")
    val integrationTestSourceSet = sourceSets.maybeCreate("integrationTest").apply {
        java.srcDir("src/integrationTest/java")
        resources.srcDir("src/integrationTest/resources")
        compileClasspath += mainSourceSet.get().output + testSourceSet.get().output
        runtimeClasspath += mainSourceSet.get().output + testSourceSet.get().output
    }

    configurations.getByName("integrationTestImplementation")
        .extendsFrom(configurations.getByName("testImplementation"))
    configurations.getByName("integrationTestRuntimeOnly")
        .extendsFrom(configurations.getByName("testRuntimeOnly"))

    val integrationTest = tasks.register<Test>("integrationTest") {
        description = "Runs Testcontainers based integration tests (requires Docker)."
        group = "verification"
        testClassesDirs = integrationTestSourceSet.output.classesDirs
        classpath = integrationTestSourceSet.runtimeClasspath
        shouldRunAfter(tasks.named("test"))
        onlyIf {
            if (!dockerReady) {
                logger.lifecycle("Skipping :${project.name}:integrationTest - Docker daemon is not available.")
            }
            dockerReady
        }
    }

    tasks.named("check") {
        dependsOn(integrationTest)
    }
}
