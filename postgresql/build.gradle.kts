plugins {
    kotlin("plugin.serialization")
    id("org.jetbrains.kotlinx.atomicfu")
}

val scramClientVersion: String by project
val kotlinCsvVersion: String by project
val kotlinxSerializationJsonVersion: String by project

dependencies {
    api(project(":core"))
    implementation("com.ongres.scram:client:$scramClientVersion")
    // https://mvnrepository.com/artifact/com.github.doyaaaaaken/kotlin-csv
    implementation("com.github.doyaaaaaken:kotlin-csv:$kotlinCsvVersion")
    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-serialization-json-jvm
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationJsonVersion")
}
