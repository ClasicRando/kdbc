plugins {
    kotlin("jvm")
    kotlin("plugin.serialization")
}

group = "com.github.clasicrando"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

buildscript {
    repositories {
        mavenCentral()
    }

    val kotlinxAtomicFuVersion: String by project

    dependencies {
        classpath("org.jetbrains.kotlinx:atomicfu-gradle-plugin:$kotlinxAtomicFuVersion")
    }
}

apply(plugin = "kotlinx-atomicfu")

val kotlinxIoVersion: String by project
val kotlinLoggingVersion: String by project
val slf4jVersion: String by project
//val kLoggingVersion: String by project
val kotlinxCoroutinesVersion: String by project
val ktorVersion: String by project
val scramClientVersion: String by project
val kotlinVersion: String by project
val kotlinxSerializationJsonVersion: String by project
val kotlinxDateTimeVersion: String by project
val kotlinxUuidVersion: String by project
val kotlinTestVersion: String by project
val junitVersion: String by project
val logbackVersion: String by project

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-io-core:$kotlinxIoVersion")
    implementation("io.github.oshai:kotlin-logging-jvm:$kotlinLoggingVersion")
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
//    implementation("io.klogging:klogging-jvm:$kLoggingVersion")
    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-coroutines-core-jvm
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:$kotlinxCoroutinesVersion")
    implementation("io.ktor:ktor-network:$ktorVersion")
    implementation("io.ktor:ktor-network-tls:$ktorVersion")
    implementation("com.ongres.scram:client:$scramClientVersion")
    implementation(kotlin("reflect", version = kotlinVersion))
    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-serialization-json-jvm
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationJsonVersion")
    api("org.jetbrains.kotlinx:kotlinx-datetime:$kotlinxDateTimeVersion")
    // https://mvnrepository.com/artifact/app.softwork/kotlinx-uuid-core
    api("app.softwork:kotlinx-uuid-core:$kotlinxUuidVersion")

    testImplementation(kotlin("test", version = kotlinTestVersion))
    testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
}

kotlin {
    jvmToolchain(11)
}

tasks.test {
    testLogging {
        setExceptionFormat("full")
        events = setOf(
            org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED,
            org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED,
            org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED,
        )
        showStandardStreams = true
        afterSuite(KotlinClosure2<TestDescriptor,TestResult,Unit>({ descriptor, result ->
            if (descriptor.parent == null) {
                println("\nTest Result: ${result.resultType}")
                println("""
                    Test summary: ${result.testCount} tests, 
                    ${result.successfulTestCount} succeeded, 
                    ${result.failedTestCount} failed, 
                    ${result.skippedTestCount} skipped
                """.trimIndent().replace("\n", ""))
            }
        }))
    }
    useJUnitPlatform()
}
