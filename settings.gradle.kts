pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }

    val kotlinVersion: String by settings
    val kotlinxSerializationPluginVersion: String by settings
    val dokkaVersion: String by settings
    val mavenPublishVersion: String by settings

    plugins {
        kotlin("jvm") version kotlinVersion
        kotlin("plugin.serialization") version kotlinxSerializationPluginVersion
        id("org.jetbrains.dokka") version dokkaVersion
        id("com.vanniktech.maven.publish") version mavenPublishVersion
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}

rootProject.name = "kdbc"
include("core")
include("postgresql")
include("benchmarks")
