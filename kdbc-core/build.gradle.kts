val kotlinVersion: String by project
val dbcpVersion: String by project
val junitVersion: String by project
val slf4Version: String by project
val kotlinLoggingVersion: String by project
val log4jVersion: String by project
val kspVersion: String by project
val coroutinesVersion: String by project
val postgresqlVersion: String by project

plugins {
    id("com.google.devtools.ksp")
    kotlin("jvm")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation(project(":kdbc-processing"))
    ksp(project(":kdbc-processing"))
    // https://mvnrepository.com/artifact/org.apache.commons/commons-dbcp2
    implementation("org.apache.commons:commons-dbcp2:$dbcpVersion")
    // https://mvnrepository.com/artifact/org.jetbrains.kotlin/kotlin-reflect
    implementation("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")
    testImplementation("org.jetbrains.kotlin:kotlin-test:$kotlinVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:$coroutinesVersion")
    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:$slf4Version")
    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:$slf4Version")
    implementation("io.github.microutils:kotlin-logging-jvm:$kotlinLoggingVersion")
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    implementation("org.postgresql","postgresql", postgresqlVersion)
    constraints {
        add("implementation", "org.apache.logging.log4j:log4j-core") {
            version {
                strictly("[$log4jVersion, 3[")
                prefer(log4jVersion)
            }
            because("CVE-2021-44228/45105/45046: Log4j vulnerable to remote code execution")
        }
        add("implementation", "org.apache.logging.log4j:log4j-api") {
            version {
                strictly("[$log4jVersion, 3[")
                prefer(log4jVersion)
            }
            because("CVE-2021-44228/45105/45046: Log4j vulnerable to remote code execution")
        }
    }
}

ksp {
}

kotlin {
    sourceSets.main {
        kotlin.srcDirs("src/main/kotlin", "build/generated/ksp/main/kotlin")
    }
}
