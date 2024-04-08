package com.github.kdbc.benchmarks.postgresql

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.Postgres
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
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

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class KdbcBenchmarkBlockingThreadPoolSingle {
    private var id = 0
    private val options: PgConnectOptions =  initializeThreadPoolBlockingConnections()
    private val executor: Executor = Executors.newWorkStealingPool()
    private val completionService = ExecutorCompletionService<Unit>(executor)

    @Setup
    open fun start() {
        Postgres.blockingConnection(connectOptions = options).use {
            it.sendQuery(setupQuery)
        }
    }

    private fun step(): Int {
        id++
        if (id > 5000) id = 1
        return id
    }

    private fun executeQuery(stepId: Int): List<PostDataClass> {
        return Postgres.blockingConnection(connectOptions = options).use { conn ->
            conn.sendPreparedStatement(kdbcQuerySingle, listOf(stepId)).use {
                val result = it.firstOrNull()
                    ?: throw Exception("No records returned from $kdbcQuerySingle, id = $stepId")
                assert(result.rowsAffected == 1L)
                result.use { qr ->
                    qr.rows.map { row ->
                        PostDataClass(
                            row.getInt(0)!!,
                            row.getString(1)!!,
                            row.getLocalDateTime(2)!!,
                            row.getLocalDateTime(3)!!,
                            row.getInt(4),
                            row.getInt(5),
                            row.getInt(6),
                            row.getInt(7),
                            row.getInt(8),
                            row.getInt(9),
                            row.getInt(10),
                            row.getInt(11),
                            row.getInt(12),
                        )
                    }
                }
            }
        }
    }

//    @Benchmark
    open fun queryData() {
        val taskCount = concurrencyLimit
        for (i in 1..taskCount) {
            val stepId = step()
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
