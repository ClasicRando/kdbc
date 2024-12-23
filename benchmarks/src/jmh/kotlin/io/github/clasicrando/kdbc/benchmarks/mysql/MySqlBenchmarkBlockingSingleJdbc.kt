package io.github.clasicrando.kdbc.benchmarks.mysql

import org.openjdk.jmh.annotations.Benchmark
import java.sql.Connection
import java.util.concurrent.TimeUnit
import kotlin.use
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

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class MySqlBenchmarkBlockingSingleJdbc {
    private var id = 0
    private val connection: Connection = getJdbcConnection()

    @Setup
    open fun start() {
        connection.createStatement().use { statement ->
            setupQueries.forEach(statement::execute)
        }
    }

    private fun singleStep() {
        id++
        if (id > 5000) id = 1
    }

    private fun multiStep() {
        id += 10
        if (id >= 5000) id = 1
    }

    @Benchmark
    open fun queryMultipleRows() {
        multiStep()
        connection.prepareStatement(query).use { preparedStatement ->
            preparedStatement.setInt(1, id)
            preparedStatement.setInt(2, id + 10)
            preparedStatement.executeQuery().use { resultSet ->
                extractPostDataClassListFromResultSet(resultSet)
            }
        }
    }

    @Benchmark
    open fun querySingleRow() {
        singleStep()
        connection.prepareStatement(querySingle).use { preparedStatement ->
            preparedStatement.setInt(1, id)
            preparedStatement.executeQuery().use { resultSet ->
                extractPostDataClassListFromResultSet(resultSet)
            }
        }
    }

    @TearDown
    open fun tearDown() {
        connection.close()
    }
}
