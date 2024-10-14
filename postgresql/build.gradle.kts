plugins {
    kotlin("plugin.serialization")
    id("org.jetbrains.kotlinx.atomicfu")
}

val scramClientVersion: String by project
val kotlinCsvVersion: String by project
val kotlinxSerializationJsonVersion: String by project
val trustStorePath: String? by project
val trustStorePassword: String? by project

dependencies {
    api(project(":core"))
    implementation("com.ongres.scram:client:$scramClientVersion")
    // https://mvnrepository.com/artifact/com.github.doyaaaaaken/kotlin-csv
    implementation("com.github.doyaaaaaken:kotlin-csv:$kotlinCsvVersion")
    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-serialization-json-jvm
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationJsonVersion")
}

tasks.test {
    trustStorePath?.let {
        systemProperty("javax.net.ssl.trustStore", it)
        systemProperty("javax.net.debug", "ssl:handshake")
    }
    trustStorePassword?.let { systemProperty("javax.net.ssl.trustStorePassword", it) }
}
