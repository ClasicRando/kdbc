plugins {
    kotlin("plugin.serialization")
    id("kotlinx-atomicfu")
}

val scramClientVersion: String by project
val kotlinCsvVersion: String by project

dependencies {
    api(project(":core"))
    implementation("com.ongres.scram:client:$scramClientVersion")
    // https://mvnrepository.com/artifact/com.github.doyaaaaaken/kotlin-csv
    implementation("com.github.doyaaaaaken:kotlin-csv:$kotlinCsvVersion")
}
