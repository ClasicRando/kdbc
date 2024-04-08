package com.github.kdbc.benchmarks.postgresql

import io.github.clasicrando.kdbc.core.connection.BlockingConnection
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
open class KdbcBenchmarkBlocking {
    private var id = 0
    private val connection: BlockingConnection = getKdbcBlockingConnection()

    @Setup
    open fun start() {
        connection.sendQuery(setupQuery)
    }

    private fun step() {
        id += 10
        if (id >= 5000) id = 1
    }

//    @Benchmark
    open fun queryData() {
        step()
        val result = connection.sendPreparedStatement(kdbcQuery, listOf(id, id + 10)).first()
        result.rows.map { row ->
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

    @TearDown
    fun destroy() {
        if (connection.isConnected) {
            try {
                connection.close()
            } catch (ex: Throwable) {
                ex.printStackTrace()
            }
        }
    }
}
