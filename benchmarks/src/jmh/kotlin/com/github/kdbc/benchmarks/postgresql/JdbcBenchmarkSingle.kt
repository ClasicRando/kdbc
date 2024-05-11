package com.github.kdbc.benchmarks.postgresql

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Warmup
import java.sql.Connection
import java.util.concurrent.TimeUnit

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class JdbcBenchmarkSingle {
    private var id = 0
    private val connection: Connection = getJdbcConnection()

    @Setup
    open fun start() {
        getJdbcConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(setupQuery)
            }
        }
    }

    private fun step() {
        id++
        if (id > 5000) id = 1
    }

    @Benchmark
    open fun queryData() {
        step()
        connection.prepareStatement(jdbcQuerySingle).use { preparedStatement ->
            preparedStatement.setInt(1, id)
            preparedStatement.executeQuery().use { resultSet ->
                extractPostDataClassListFromResultSet(resultSet)
            }
        }
    }

    @TearDown
    open fun destroy() {
        connection.close()
    }
}
