plugins {
    kotlin("plugin.serialization")
    id("kotlinx-atomicfu")
}

group = "com.github.clasicrando"
version = "0.1"

val scramClientVersion: String by project

dependencies {
    api(project(":common"))
    implementation("com.ongres.scram:client:$scramClientVersion")
}
