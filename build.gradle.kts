import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.SonatypeHost

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
    version = "0.0.2"

    repositories {
        mavenCentral()
    }

    kotlin {
        jvmToolchain(11)
        compilerOptions.optIn.add("kotlin.contracts.ExperimentalContracts")
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
    val kotlinxUuidVersion: String by project
    val kotlinTestVersion: String by project
    val junitVersion: String by project
    val logbackVersion: String by project
    val mockkVersion: String by project

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
        api("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationJsonVersion")
        api("org.jetbrains.kotlinx:kotlinx-datetime:$kotlinxDateTimeVersion")
        // https://mvnrepository.com/artifact/app.softwork/kotlinx-uuid-core
        api("app.softwork:kotlinx-uuid-core:$kotlinxUuidVersion")

        testImplementation(kotlin("test", version = kotlinTestVersion))
        testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
        testImplementation("io.mockk:mockk:$mockkVersion")
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
