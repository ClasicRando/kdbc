plugins {
    kotlin("plugin.serialization")
    id("kotlinx-atomicfu")
}

group = "io.github.clasicrando"
version = "0.1"

val scramClientVersion: String by project

dependencies {
    api(project(":core"))
    implementation("com.ongres.scram:client:$scramClientVersion")
}
