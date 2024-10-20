package io.github.clasicrando.kdbc.benchmarks.postgresql

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class PgBenchmarkBlockingMultiJdbc {
    private var id = 0
    private val dataSource: DataSource = getJdbcDataSource()
    private val executor: Executor = Executors.newWorkStealingPool()
    private val completionService = ExecutorCompletionService<Unit>(executor)

    @Setup
    open fun start() {
        getJdbcConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(setupQuery)
            }
        }
    }

    private fun singleStep(): Int {
        id++
        if (id > 5000) id = 1
        return id
    }

    private fun executeQuery(stepId: Int) {
        dataSource.connection.use { conn ->
            conn.prepareStatement(jdbcQuerySingle).use { preparedStatement ->
                preparedStatement.setInt(1, stepId)
                preparedStatement.executeQuery().use { resultSet ->
                    extractPostDataClassListFromResultSet(resultSet)
                }
            }
        }
    }

    @Benchmark
    open fun querySingleRow() {
        val taskCount = concurrencyLimit
        repeat(taskCount) {
            val stepId = singleStep()
            completionService.submit { executeQuery(stepId) }
        }
        var received = 0
        while (received < taskCount) {
            val future = completionService.take()
            future.get()
            received++
        }
    }
}