import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.SonatypeHost

plugins {
    kotlin("jvm")
    id("com.vanniktech.maven.publish")
    signing
    id("org.jetbrains.dokka")
    id("org.jetbrains.kotlinx.atomicfu")
    id("com.ncorti.ktfmt.gradle")
}

allprojects {
    apply(plugin = "kotlin")
    group = "io.github.clasicrando"
    version = "0.0.4"

    repositories {
        mavenCentral()
    }

    kotlin {
        jvmToolchain(17)
        compilerOptions.optIn.add("kotlin.contracts.ExperimentalContracts")
        compilerOptions.optIn.add("kotlin.uuid.ExperimentalUuidApi")
        compilerOptions.optIn.add("io.github.clasicrando.kdbc.core.annotations.InternalApi")
        compilerOptions {
            freeCompilerArgs.add("-Xwhen-guards")
        }
        if (this@allprojects.name != "benchmarks") {
            explicitApi()
        }
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlinx.atomicfu")
    apply(plugin = "signing")
    apply(plugin = "org.jetbrains.dokka")
    apply(plugin = "com.vanniktech.maven.publish")
    apply(plugin = "com.ncorti.ktfmt.gradle")

    repositories {
        mavenCentral()
    }

    val kotlinxIoVersion: String by project
    val kotlinLoggingVersion: String by project
    val slf4jVersion: String by project
    val coroutinesVersion: String by project
    val ktorVersion: String by project
    val kotlinVersion: String by project
    val serializationJsonVersion: String by project
    val junitVersion: String by project
    val logbackVersion: String by project
    val mockkVersion: String by project

    dependencies {
        implementation("org.jetbrains.kotlinx:kotlinx-io-core:$kotlinxIoVersion")
        implementation("io.github.oshai:kotlin-logging-jvm:$kotlinLoggingVersion")
        implementation("org.slf4j:slf4j-api:$slf4jVersion")
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:$coroutinesVersion")
        implementation("io.ktor:ktor-network:$ktorVersion")
        implementation("io.ktor:ktor-network-tls:$ktorVersion")
        implementation(kotlin("stdlib", version = kotlinVersion))
        implementation(kotlin("reflect", version = kotlinVersion))
        implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$serializationJsonVersion")

        testImplementation(kotlin("test", version = kotlinVersion))
        testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
        testImplementation("io.mockk:mockk:$mockkVersion")
    }

    ktfmt {
        kotlinLangStyle()
    }

    tasks.test {
        workingDir = project.rootDir
        testLogging {
            setExceptionFormat("full")
            events =
                setOf(
                    org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED,
                    org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED,
                    org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED,
                )
            showStandardStreams = true
            afterSuite(
                KotlinClosure2<TestDescriptor, TestResult, Unit>({ descriptor, result ->
                    if (descriptor.parent == null) {
                        println("\nTest Result: ${result.resultType}")
                        println(
                            """
                            Test summary: ${result.testCount} tests,
                            ${result.successfulTestCount} succeeded,
                            ${result.failedTestCount} failed,
                            ${result.skippedTestCount} skipped
                            """.trimIndent().replace("\n", ""),
                        )
                    }
                }),
            )
        }
        useJUnitPlatform()
    }

    tasks.dokkaHtml {
        outputDirectory.set(layout.buildDirectory.dir("documentation/html"))
    }

    tasks.dokkaJavadoc {
        outputDirectory.set(layout.buildDirectory.dir("documentation/javadoc"))
    }

    val projName =
        when (project.name) {
            "core" -> "kdbc-core"
            "postgresql" -> "kdbc-postgresql"
            else -> "kdbc-other"
        }

    tasks {
        jar {
            base.archivesName.set(projName)
        }
    }

    mavenPublishing {
        configure(
            KotlinJvm(
                javadocJar = JavadocJar.Dokka("dokkaHtml"),
                sourcesJar = true,
            ),
        )
        publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
        coordinates(group.toString(), projName, version.toString())
        pom {
            name.set("kdbc")
            description.set("Blocking and Non-Blocking database drivers using Kotlin")
            inceptionYear.set("2024")
            url.set("https://github.com/ClasicRando/kdbc")
            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    distribution.set("repo")
                }
            }
            developers {
                developer {
                    name.set("Steven Thomson")
                    email.set("steventhomson9@gmail.com")
                }
            }
            scm {
                url.set("https://github.com/ClasicRando/kdbc/tree/main")
                connection.set("scm:git:git://github.com/ClasicRando/kdbc.git")
                developerConnection.set("scm:git:ssh://github.com:ClasicRando/kdbc.git")
            }
        }
        signAllPublications()
    }
}
