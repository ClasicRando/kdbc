pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }

    val kotlinVersion: String by settings
    val kotlinxSerializationPluginVersion: String by settings

    plugins {
        kotlin("jvm") version kotlinVersion
        kotlin("plugin.serialization") version kotlinxSerializationPluginVersion
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}

rootProject.name = "kdbc"
include("core")
findProject("core")?.name = "kdbc-core"
include("postgresql")
findProject("postgresql")?.name = "kdbc-postgresql"
include("benchmarks")
