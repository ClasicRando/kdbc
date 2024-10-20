plugins {
    id("me.champeau.jmh") version ("0.7.2")
}

group = "com.github.clasicrando"
version = "0.1"

val kotlinCsvVersion: String by project

dependencies {
    implementation(project(":postgresql"))
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    implementation("org.postgresql:postgresql:42.7.3")
    // https://mvnrepository.com/artifact/org.apache.commons/commons-dbcp2
    implementation("org.apache.commons:commons-dbcp2:2.12.0")
    // https://mvnrepository.com/artifact/com.github.doyaaaaaken/kotlin-csv
    implementation("com.github.doyaaaaaken:kotlin-csv:$kotlinCsvVersion")
}

jmh {
    dependencies {
        jmhImplementation("org.openjdk.jmh:jmh-core:1.36")
        jmhImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.36")
        jmhAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.36")
    }
    resultsFile =
        rootProject.rootDir
            .resolve("benchmark-results")
            .resolve("results.txt")
}
