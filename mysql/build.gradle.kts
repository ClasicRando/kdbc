plugins {
    kotlin("plugin.serialization")
    id("org.jetbrains.kotlinx.atomicfu")
}

dependencies {
    api(project(":core"))
}

kotlin {
    compilerOptions.optIn.add("kotlin.ExperimentalStdlibApi")
}
