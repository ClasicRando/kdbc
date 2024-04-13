<p align="center">
  <img src="kdbc.png"  alt="kdbc Logo"/>
  &ensp;&ensp;&ensp;
  <img height="50px" src="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fkotlinlang.org%2Fdocs%2Fimages%2Fkotlin-logo.png&f=1&nofb=1&ipt=f9985bbac1e2117b69f4a0950f0dea95714d864df42c33c0c03705286124e127&ipo=images" alt="kotlin logo">
</p>

<div align="center">
    <h4>
        <a href="https://github.com/ClasicRando/kdbc/wiki">Wiki</a>
    </h4>
</div>

Database drivers using Kotlin™ to drive database communication. The library
provides both blocking and non-blocking solutions for database communication.
The coroutine based solution uses Ktor networking under the hood which
implements non-blocking selector socket channel connections. For the blocking
solution, basic java sockets (as seen in JDBC based libraries) are used to
facilitate database communications. This library is heavily inspired by Rust's
[SQLx](https://github.com/launchbadge/sqlx) and the connection spec in C#.

Currently, there is only initial support for [Postgresql](https://www.postgresql.org/) and plans for
other freely available databases, but other JDBC compliant databases might
also be added in the future.

## Importing Library
### Gradle
```kotlin
dependencies {
    implementation("io.github.clasicrando:kdbc-postgresql:0.0.1")
}
```

## Basic Usage
```kotlin
val connectOptions = PgConnectOptions(
      host = "localhost",
      port = 5342U,
      username = "username",
      password = "yourSecretPassword",
      applicationName = "MyFirstKdbcProject"
)
val connection = Postgres.connection(connectOption = connectOptions)
val text: String = connection.query("SELECT 'KDBC Docs'").fetchScalar()
println(text) // KDBC Docs
connection.query("CALL your_stored_procedure($1, $2)")
    .bind(1)
    .bind("KDBC Docs")
    .execute()
connection.close()
```
