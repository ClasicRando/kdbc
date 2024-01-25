plugins {
    id("me.champeau.jmh") version ("0.7.2")
}

group = "com.github.clasicrando"
version = "0.1"

dependencies {
    implementation(project(":postgresql"))
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    implementation("org.postgresql:postgresql:42.7.1")
}

jmh {
    dependencies {
        jmhImplementation("org.openjdk.jmh:jmh-core:1.36")
        jmhImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.36")
        jmhAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.36")
    }
}
