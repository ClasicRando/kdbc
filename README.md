<p align="center">
  <img src="kdbc.png"  alt="kdbc Logo"/>
  &ensp;&ensp;&ensp;
  <img height="50px" src="https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fkotlinlang.org%2Fdocs%2Fimages%2Fkotlin-logo.png&f=1&nofb=1&ipt=f9985bbac1e2117b69f4a0950f0dea95714d864df42c33c0c03705286124e127&ipo=images" alt="kotlin logo">
</p>

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

## Basic Usage
### Query API
Usage with the high level Query API
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
connection.close()
```

Like most database drivers, it is recommended to allows close connections as
soon as you are done with them. This can be done with a call to
`Connection.close()` but like with `AutoCloseable` objects in Kotlin, there is
an extension method `Connection.use` which will always close a connection when
the scope of the method ends (including non-local returns).
```kotlin
val connectOptions = PgConnectOptions(
      host = "localhost",
      port = 5342U,
      username = "username",
      password = "yourSecretPassword",
      applicationName = "MyFirstKdbcProject"
)
// The connection is provided as an argument to the inlined lambda and the connection is always
// closed before exiting the block 
Postgres.connection(connectOption = connectOptions).use { conn: PgConnection ->
    val text: String = conn.query("SELECT 'KDBC Docs'").fetchScalar()
    println(text) // KDBC Docs
}
```

Using the Query API you can bind parameters to the statement to send data to
the server with the query.
```kotlin
val connectOptions = PgConnectOptions(
      host = "localhost",
      port = 5342U,
      username = "username",
      password = "yourSecretPassword",
      applicationName = "MyFirstKdbcProject"
)
// The connection is provided as an argument to the inlined lambda and the connection is always
// closed before exiting the block
Postgres.connection(connectOption = connectOptions).use { conn: PgConnection ->
    conn.query("CALL your_stored_procedure($1, $2)")
        .bind(1)
        .bind("KDBC Docs")
        .execute()
}
```

The Query API also provides the ability to parse rows directly using a
`RowParser<T>` which defines a method to convert a `DataRow` into the required
type `T`. The hope is to make this into a compiler plugin so `RowParser<T>`
implementations are auto generated in the future. Either that or we hook into
the kotlinx Serialization library to get compiler support.
```kotlin
data class TableRow(val id: Int, val text: String)

object TableRowParser : RowParser<TableRow> {
    override fun fromRow(row: DataRow): Row {
        return TableRow(
          id = row.getInt(0)!!,
          text = getStringOrThrow(row, "string_value")
        )
    }
}

val connectOptions = PgConnectOptions(
  host = "localhost",
  port = 5342U,
  username = "username",
  password = "yourSecretPassword",
  applicationName = "MyFirstKdbcProject"
)
// The connection is provided as an argument to the inlined lambda and the connection is always
// closed before exiting the block
Postgres.connection(connectOption = connectOptions).use { conn: PgConnection ->
    val rows = conn.query("SELECT 1 id, 'KDBC Docs' string_value").fetchAll(TableRowParser)
    println(rows.joinToString(prefix = "[", postfix = "]")) // [TableRow(id=1,text=KDBC Docs)]
}
```

### Connection Pooling
By default, connections are pooled based upon unique connect options provided
to the database vendor's specific `Connection.connect()` methods. This means
that you don't need to worry about making `javax.sql.DataSource` instances and
using them. However, you do need to configure pooling options in the connect
options if you want to override the various database connection specific
default behaviour. In our opinion, the trade-offs are worth it because it means
you get to reuse physical TCP connections by default, so you might only need to
create 1 connection during the lifecycle of your application and never have to
create another one again.

### Low level API
Usage with the low level API that uses `StatementResult` which is a collection
of `QueryResult`s that contains rows as a `ResultSet`. Each of these types are
iterable allowing you to use hook into the extension methods the Kotlin stdlib
provides.
```kotlin
val connectOptions = PgConnectOptions(
    host = "localhost",
    port = 5342U,
    username = "username",
    password = "yourSecretPassword",
    applicationName = "MyFirstKdbcProject"
)
val connection = Postgres.connection(connectOption = connectOptions)
val statementResult = connection.sendQuery("SELECT 'KDBC Docs'")
val queryResult = statementResult[0]
println(queryResult.rowAffected) // 1
println(queryResult.rows.columnCount) // 1
val rows = queryResult.toList()
println(rows.size) // 1
println(rows[0].getString(0)) // KDBC Docs
```

### Blocking Connections
Although the main focus of this library is to let Kotlin shine with suspending
functions, there is also a `BlockingConnection`, `BlockingQuery` and
`BlockingQueryBatch` flavours for those that don't need function suspending
(e.g. CLI applications that do not utilize multi-threading). These
implementations closely resemble their suspending comrades but with
non-suspending functions and blocking collection types (i.e. `Sequence` over
`Flow`). Most of the codebase does not directly depend upon suspension, such as
encoding and decoding values, so most of the code can be shared.

### SSL
Unfortunately, I have yet to fully understand how to implement SSL using Ktor
or Java sockets, but we have that as a feature that is needed before this
library can be used more broadly.

## Database Vendor Specific Behaviour
Since databases have their own behaviour that make them special, vendor
specific functionality is also supported on `Connection` and
`BlockingConnection` implementations.

### Postgresql
#### Type Mapping
| postgresql                      | Kotlin                          |
|---------------------------------|---------------------------------|
| boolean                         | Boolean                         |
| "char"                          | Byte                            |
| smallint, int2, smallserial     | Short                           |
| int, int4, serial               | Int                             |
| bigint, int8, bigserial         | Long                            |
| real, float4                    | Float                           |
| double precision, float8        | Double                          |
| varchar(n), text, name, char(n) | String                          |
| bytea                           | ByteArray                       |
| void                            | Unit                            |
| interval                        | kotlinx.datetime.DateTimePeriod |
| money                           | PgMoney*                        |
| numeric                         | java.math.BigDecimal            |
| timestamp                       | kotlinx.datetime.LocalDateTime  |
| timestamptz                     | DateTime*                       |
| date                            | kotlinx.datetime.LocalDate      |
| time                            | kotlinx.datetime.LocalTime      |
| timetz                          | PgTimeTz*                       |
| uuid                            | kotlinx.uuid.UUID               |
| inet                            | PgInet*                         |
| json, jsonb                     | PgJson*                         |

\* types that are specific to the library

#### Array Types
Array variants for all types are supported for 1 dimensional arrays and decode
to a `List<T>` for the desired type.

#### Composite Types
Composite types are useful when trying to represent the contents of an entire
table row or for user defined types that are not existing tables. These are
supported but with a few conditions.

- data classes **ONLY**
- every field in the data class must be types that can be serialized into and
deserialized from the database
- the type must be registered with a connection to be used (once they are
registered, the type is available within all the adjacent pooled connections
for the lifetime of the pool which is generally the application life)

Take this composite definition:
```postgres-psql
CREATE TYPE my_type AS (
    id int,
    text_value text
);
```

That can be represented as a data class and registered with the connections.
```kotlin
data class MyType(val id: Int, val textValue: String)

val connection = Postgres.connection(connectOptions)
connection.registerCompositeType<MyType>("my_type")
```

Currently, this uses reflection to find the properties of the type when
registering and create new instances when deserializing. In the future we would
want to use a compiler plugin to generate and register these type globally. 

#### Enum Types
Kotlin Enums can be used to represent custom enums in a postgresql database.
Take this enum definition:
```postgres-psql
CREATE TYPE my_enum AS ENUM ('First', 'Second', 'Third');
```

That can be represented as an enum class and registered with the connections.
```kotlin
enum class MyEnum {
    First,
    Second,
    Third,
}

val connection = Postgres.connection(connectOptions)
connection.registerEnumType<MyEnum>("my_enum")
```

This uses the `enumValues<E>()` to get the first match to the enum text value
exactly. It also writes the `Enum.name` exactly so make sure they match the
database value.

#### Copy
Copy statements are supported for `COPY FROM` and `COPY TO`. The API depends on
if you are using a `PgConnection` or `PgBlockingConnection` since the
suspending connection works with `Flow`s while the blocking version works with
`Sequence`s. There is also a custom type, `CopyStatement` that is used in place
of a hand created copy query. `CopyStatement` provides all the features that a
raw copy query provides but with some guardrails to avoid issues (such as
specifying the wrong value for options).

To interact with the copy API, there are methods on `PgConnection` and
`PgBlockingConnection` called:

- `copyIn`, accepts a `CopyStatement` and `Flow` of data, finally executing a `COPY FROM` query with the data `Flow` to write all the data to the target table 
- `copyOut`, accepts a `CopyStatement` and executes a `COPY TO` query to read all data from the target table

In both cases the only COPY type supported is text variants (i.e. binary
encoding is not supported....yet!) and data is always in `ByteArray` format so
the user must encode data into `ByteArray`s to represent the ingested text data
and decode the received `ByteArray`s as text data. Future improvements would be
to allow encoding and decoding data as rows.

```postgres-psql
CREATE TABLE test_table (
    id int,
    text_value text
) AS
SELECT 1, 'SQL data';
```
```kotlin
val rows = flowOf(
    "2,KDBC Docs\n".toByteArray(),
    "3,Extra Row\n".toByteArray(),
)
val copyStatement = CopyStatement(
    tableName = "test_table",
    schemaname = "public",
    columns = listOf(),
    format = CopyFormat.CSV,
)
Postgres.connection(connectOptions).use { conn: PgConnection ->
    conn.copyIn(copyStatement, rows)
    /*
        This prints:
        1,SQL data
        2,KDBC Docs
        3,Extra Row
     */
    conn.copyOut(copyStatement).collect { row: ByteArray ->
        print(String(row))
    }
}
```

#### Listen/Notify
Postgresql databases support a `LISTEN/NOTIFY` protocol to subscribe to a
desired channel and publish asynchronous messages to subscribers. This is
supported for both blocking and suspending connection variants but differ in
how notifications are collected. For `PgConnection`, messages are parsed as
they arrive to the client so notifications are always accessible through
`PgConnection.notifications` as a `ReceiveChannel`. This means you can listen
to messages outside the regular message flow in suspending connection. For
`PgBlockingConnection`, messages are parsed as needed so the connection needs
to flush the message buffer to ensure all messages have been received before
returning any pending messages to the caller. This means the connection will be
blocked during this operation whereas the suspending connection will be free to
execute queries as the caller tries to receive the next notification.
```kotlin
// Suspending Variant (receive)
val connection = Postgres.connection(connectOptions)
// Suspend until the next notification is available
val notification: PgNotification = connection.notifications.receive()
println(notification)

// Suspending Variant (loop)
val connectionLoop = Postgres.connection(connectOptions)
// Loops until the channel is closed
for (notification in connectionLoop.notifications) {
    println(notification)
}

// Blocking
val blockingConnection = PgBlockingConnection.connect(connectOptions)
// Receives all pending notifications
val notifications = blockingConnection.getNotifications()
for (notification in connectionLoop.notifications) {
    println(notification)
}
```

#### PostGIS
PostGIS types are supported by calling the `includePostGisTypes` method on both
connection flavours. This adds PostGIS types to the pooled type registry so any
connection in the same pool will have the types available.

| postgresql | Kotlin        |
|------------|---------------|
| box        | PgBox         |
| circle     | PgCircle      |
| line       | PgLine        |
| lseg       | PgLineSegment |
| path       | PgPath        |
| point      | PgPoint       |
| polygon    | PgPolygon     |

All Kotlin types are found in this library

#### Query Pipelining
A seldom referenced but powerful feature for postgres is to pipeline prepared
queries to reduce overall query time when executing multiple queries one after
another. Although the server receives and processes query requests
sequentially, all queries can be sent for processing and the results are pooled
into a single result to avoid continuous call and response messages from the
server between queries.

To execute queries pipelined, either use the `pipelineQueries` method on the
postgresql connection object, or use the batch query API which utilizes query
pipelining for postgresql connections.

One big limitation to using pipelined queries is how the server treats each
query and the transactional nature of the querying. To avoid overly complex
client logic, the query pipeline API is an all or nothing machine. This means
that when executing the queries, either every query is a separate transactional
unit or all queries are grouped into a single transaction where 1 failure
causes all other queries to be aborted. However, this can be avoided when the
connection is already in an open transaction since the server does not
implicitly commit/rollback transaction when already in a transaction block.

<table>
  <thead>
    <tr><th>Regular</th><th>Pipelined</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>
        <table>
            <tr>
                <td>Client</td>
                <td>Server</td>
            </tr>
            <tr>
                <td>send query 1</td>
                <td></td>
            </tr>
            <tr>
                <td></td>
                <td>process query 1</td>
            </tr>
            <tr>
                <td>receive rows 1</td>
                <td></td>
            </tr>
            <tr>
                <td>send query 2</td>
                <td></td>
            </tr>
            <tr>
                <td></td>
                <td>process query 2</td>
            </tr>
            <tr>
                <td>receive rows 2</td>
                <td></td>
            </tr>
            <tr>
                <td>send query 3</td>
                <td></td>
            </tr>
            <tr>
                <td></td>
                <td>process query 3</td>
            </tr>
            <tr>
                <td>receive rows 3</td>
                <td></td>
            </tr>
        </table>
      </td>
      <td>
        <table>
            <tr>
                <td>Client</td>
                <td>Server</td>
            </tr>
            <tr>
                <td>send query 1</td>
                <td></td>
            </tr>
            <tr>
                <td>send query 2</td>
                <td>process query 1</td>
            </tr>
            <tr>
                <td>send query 3</td>
                <td>process query 2</td>
            </tr>
            <tr>
                <td>receive rows 1</td>
                <td>process query 3</td>
            </tr>
            <tr>
                <td>receive rows 2</td>
                <td></td>
            </tr>
            <tr>
                <td>receive rows 3</td>
                <td></td>
            </tr>
        </table>
      </td>
    </tr>
  </tbody>
</table>
