/*
 * order-contract: the only thing both services depend on.
 *
 * Keeping topic names and the event payload in one tiny library guarantees that the producer and
 * the consumer can never disagree about the wire format, while staying free of any Spring code.
 */
plugins {
    id("java-library")
}

dependencies {
    // The payload is (de)serialized with Jackson on both sides, so the annotations ship with it.
    "api"("com.fasterxml.jackson.core:jackson-databind")
    "api"("com.fasterxml.jackson.core:jackson-annotations")
    "api"("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    "testImplementation"(libs.spring.boot.starter.test)
}
