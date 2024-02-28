package com.github.clasicrando.benchmarks.postgresql

import com.github.jasync.sql.db.pool.ConnectionPool
import com.github.jasync.sql.db.postgresql.PostgreSQLConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.toKotlinLocalDateTime
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
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class JasyncBenchmarkSingle {
    private var id = 0
    private val pool: ConnectionPool<PostgreSQLConnection> = getJasyncPool()

    @Setup
    open fun start(): Unit = runBlocking {
        pool.sendQuery(setupQuery).await()
    }

    private fun step() {
        id++
        if (id > 5000) id = 1
    }

//    @Benchmark
    open fun queryData() = runBlocking(Dispatchers.IO) {
        step()
        val result = pool.sendPreparedStatement(jdbcQuerySingle, listOf(id)).await()
        val row = result.rows.first()
        PostDataClass(
            row.getInt(0)!!,
            row.getString(1)!!,
            row.getAs<java.time.LocalDateTime>(2).toKotlinLocalDateTime(),
            row.getAs<java.time.LocalDateTime>(3).toKotlinLocalDateTime(),
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

    @TearDown
    fun destroy() = runBlocking {
        if (pool.isConnected()) {
            try {
                pool.disconnect().await()
            } catch (ex: Throwable) {
                ex.printStackTrace()
            }
        }
    }
}
