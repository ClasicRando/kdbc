package com.github.clasicrando.benchmarks.postgresql

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.pool.KdbcPoolsManager
import com.github.clasicrando.common.result.getDateTime
import com.github.clasicrando.common.result.getInt
import com.github.clasicrando.common.result.getString
import com.github.clasicrando.postgresql.connection.PgConnectOptions
import com.github.clasicrando.postgresql.connection.PgConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
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
import java.util.concurrent.TimeUnit

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class KdbcBenchmarkConcurrentSingle {
    private var id = 0
    private val options: PgConnectOptions = runBlocking { initializeConcurrentConnections() }

    @Setup
    open fun start(): Unit = runBlocking {
        PgConnection.connect(connectOptions = options).use {
            it.sendQuery(setupQuery)
        }
    }

    private fun step(): Int {
        id++
        if (id > 5000) id = 1
        return id
    }

//    @Benchmark
    open fun queryData() = runBlocking(Dispatchers.IO) {
        val result = (1..concurrencyLimit).map {
            val stepId = step()
            async {
                PgConnection.connect(connectOptions = options).use {
                    val result = it.sendPreparedStatement(kdbcQuerySingle, listOf(stepId)).firstOrNull()
                        ?: throw Exception("No records returned from $kdbcQuerySingle, id = $stepId")
                    result.rows.map { row ->
                        PostDataClass(
                            row.getInt(0)!!,
                            row.getString(1)!!,
                            row.getDateTime(2)!!.datetime,
                            row.getDateTime(3)!!.datetime,
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
        result.awaitAll()
    }

    @TearDown
    fun destroy() = runBlocking {
        KdbcPoolsManager.closeAllPools()
    }
}
