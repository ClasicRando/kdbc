import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.SonatypeHost
import org.gradle.internal.os.OperatingSystem
import java.io.ByteArrayOutputStream
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration

val podmanExecutable = "podman"
val postgresPort: String by project
val postgresPassword: String by project

data class Container(
    val name: String,
    val externalPort: Int,
    val internalPort: Int,
    val image: String,
    val startupWaitTime: Duration = 20.toDuration(unit = DurationUnit.SECONDS),
    val startUpWaitSleepTime: Duration = 1.toDuration(unit = DurationUnit.SECONDS),
    val environmentVariables: Map<String, String> = mapOf(),
    val startupTest: Container.() -> Unit,
) {
    fun runContainer(task: Task) {
        val arguments = mutableListOf(
            "create",
            "--name",
            name,
            "-p",
            "$externalPort:$internalPort"
        )
        if (environmentVariables.isNotEmpty()) {
            arguments.add("-e")
        }
        for ((key, value) in environmentVariables) {
            arguments.add("$key=$value")
        }
        arguments.add(image)

        task.logger.info("Creating test container '$name'")
        execCommand(
            executable = podmanExecutable,
            args = arguments,
        )
        execCommand(
            executable = podmanExecutable,
            args = listOf("start", name),
        )

        task.logger.info("Waiting for test container '$name' to start")
        val end = Instant.now().plusMillis(startupWaitTime.inWholeMilliseconds)
        while (true) {
            if (Instant.now().isAfter(end)) {
                error("Exceeded timeout to validate container startup")
            }
            try {
                startupTest(this)
                break
            } catch (ex: Exception) {
                task.logger.debug(ex.message)
                Thread.sleep(1000)
            }
        }
    }

    fun removeContainer(task: Task) {
        task.logger.info("Removing test container '$name'")
        execCommand(
            executable = podmanExecutable,
            args = listOf("container", "stop", name),
            ignoreError = true,
        )
        execCommand(
            executable = podmanExecutable,
            args = listOf("container", "rm", name),
            ignoreError = true,
        )
    }
}

val containers = listOf(
    Container(
        name = "kdbc-test-pg",
        externalPort = postgresPort.toInt(),
        internalPort = 5432,
        image = "postgis/postgis",
        environmentVariables = mapOf("POSTGRES_PASSWORD" to postgresPassword)
    ) {
        execCommand(
            executable = "psql",
            args = listOf(
                "-h",
                "127.0.0.1",
                "-p",
                postgresPort,
                "-U",
                "postgres",
                "-c",
                "SELECT 1",
            )
        )
    }
)

fun Task.startContainers() {
    val os = OperatingSystem.current()
    if (os.isWindows) {
        execCommand(
            executable = podmanExecutable,
            args = listOf("machine", "start"),
        )
    } else if (os.isLinux) {
        execCommand(
            executable = "command",
            args = listOf("-v", podmanExecutable),
        )
    } else {
        error("Tests currently not supported on OS = $os")
    }

    stopContainers()
    containers.forEach { c -> c.runContainer(this) }
}

fun Task.stopContainers() = containers.forEach { c -> c.removeContainer(this) }

fun execCommand(
    executable: String,
    args: List<String>,
    ignoreError: Boolean = false,
) {
    val output = ByteArrayOutputStream()
    val errorOutput = ByteArrayOutputStream()
    val result = exec {
        this.executable = executable
        this.args = args
        standardOutput = output
        this.errorOutput = errorOutput
        isIgnoreExitValue = true
    }
    if (result.exitValue == 0 || ignoreError) {
        return
    }
    error("${output.toString(Charsets.UTF_8)}\n${errorOutput.toString(Charsets.UTF_8)}".trim())
}

val isLocalTest: Boolean get() = System.getenv("LOCAL_TEST")?.toBoolean() ?: true

plugins {
    kotlin("jvm")
    id("com.vanniktech.maven.publish")
    signing
    id("org.jetbrains.dokka")
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

allprojects {
    apply(plugin = "kotlin")
    group = "io.github.clasicrando"
    version = "0.0.3"

    repositories {
        mavenCentral()
    }

    kotlin {
        jvmToolchain(17)
        compilerOptions.optIn.add("kotlin.contracts.ExperimentalContracts")
        compilerOptions.optIn.add("kotlin.uuid.ExperimentalUuidApi")
    }
}

subprojects {
    apply(plugin = "kotlinx-atomicfu")
    apply(plugin = "signing")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "com.vanniktech.maven.publish")

    repositories {
        mavenCentral()
    }

    val kotlinxIoVersion: String by project
    val kotlinLoggingVersion: String by project
    val slf4jVersion: String by project
    val kotlinxCoroutinesVersion: String by project
    val ktorVersion: String by project
    val kotlinVersion: String by project
    val kotlinxSerializationJsonVersion: String by project
    val kotlinxDateTimeVersion: String by project
    val kotlinTestVersion: String by project
    val junitVersion: String by project
    val logbackVersion: String by project
    val mockkVersion: String by project
    val bigNumVersion: String by project

    dependencies {
        implementation("org.jetbrains.kotlinx:kotlinx-io-core:$kotlinxIoVersion")
        implementation("io.github.oshai:kotlin-logging-jvm:$kotlinLoggingVersion")
        implementation("org.slf4j:slf4j-api:$slf4jVersion")
        // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-coroutines-core-jvm
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:$kotlinxCoroutinesVersion")
        implementation("io.ktor:ktor-network:$ktorVersion")
        implementation(kotlin("stdlib", version = kotlinVersion))
        implementation(kotlin("reflect", version = kotlinVersion))
        // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-serialization-json-jvm
        implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationJsonVersion")
        api("org.jetbrains.kotlinx:kotlinx-datetime:$kotlinxDateTimeVersion")
        // https://mvnrepository.com/artifact/com.ionspin.kotlin/bignum
        api("com.ionspin.kotlin:bignum:$bigNumVersion")

        testImplementation(kotlin("test", version = kotlinTestVersion))
        testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
        testImplementation("io.mockk:mockk:$mockkVersion")
    }

    tasks.test {
        workingDir = project.rootDir
        if (isLocalTest) {
            environment("PGPASSWORD" to postgresPassword)
        }
        environment("PG_TEST_PASSWORD" to postgresPassword)
        environment("PG_TEST_PORT" to postgresPort)
        doFirst {
            if (isLocalTest) {
                startContainers()
            }
        }
        testLogging {
            setExceptionFormat("full")
            events = setOf(
                org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED,
                org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED,
                org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED,
            )
            showStandardStreams = true
            afterSuite(KotlinClosure2<TestDescriptor, TestResult, Unit>({ descriptor, result ->
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
        doLast {
            if (isLocalTest) {
                stopContainers()
            }
        }
    }

    tasks.dokkaHtml {
        outputDirectory.set(layout.buildDirectory.dir("documentation/html"))
    }

    tasks.dokkaJavadoc {
        outputDirectory.set(layout.buildDirectory.dir("documentation/javadoc"))
    }

    val projName = when (project.name) {
        "core" -> "kdbc-core"
        "postgresql" -> "kdbc-postgresql"
        else -> "kdbc-other"
    }

    tasks {
        jar {
            base.archivesName = projName
        }
    }

    mavenPublishing {
        configure(KotlinJvm(
            javadocJar = JavadocJar.Dokka("dokkaHtml"),
            sourcesJar = true,
        ))
        publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
        coordinates(group.toString(), projName, version.toString())
        pom {
            name = "kdbc"
            description = "Blocking and Non-Blocking database drivers using Kotlin"
            inceptionYear = "2024"
            url = "https://github.com/ClasicRando/kdbc"
            licenses {
                license {
                    name = "The Apache License, Version 2.0"
                    url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    distribution = "repo"
                }
            }
            developers {
                developer {
                    name = "Steven Thomson"
                    email = "steventhomson9@gmail.com"
                }
            }
            scm {
                url = "https://github.com/ClasicRando/kdbc/tree/main"
                connection = "scm:git:git://github.com/ClasicRando/kdbc.git"
                developerConnection = "scm:git:ssh://github.com:ClasicRando/kdbc.git"
            }
        }
        signAllPublications()
    }
}
