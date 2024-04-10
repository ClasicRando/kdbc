plugins {
    kotlin("plugin.serialization")
    id("kotlinx-atomicfu")
}

val scramClientVersion: String by project

dependencies {
    api(project(":core"))
    implementation("com.ongres.scram:client:$scramClientVersion")
}
