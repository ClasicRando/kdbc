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
val kLoggingVersion: String by project
val kotlinxCoroutinesVersion: String by project
val ktorVersion: String by project
val scramClientVersion: String by project
val kotlinVersion: String by project
val kotlinxSerializationJsonVersion: String by project
val kotlinxDateTimeVersion: String by project
val kotlinxUuidVersion: String by project

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-io-core:$kotlinxIoVersion")
    implementation("io.klogging:klogging-jvm:$kLoggingVersion")
    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-coroutines-core-jvm
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:$kotlinxCoroutinesVersion")
    implementation("io.ktor:ktor-network:$ktorVersion")
    implementation("com.ongres.scram:client:$scramClientVersion")
    implementation(kotlin("reflect", version = kotlinVersion))
    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-serialization-json-jvm
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationJsonVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:$kotlinxDateTimeVersion")
    // https://mvnrepository.com/artifact/app.softwork/kotlinx-uuid-core
    implementation("app.softwork:kotlinx-uuid-core:$kotlinxUuidVersion")
}

kotlin {
    jvmToolchain(11)
}