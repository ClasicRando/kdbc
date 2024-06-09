package com.github.kdbc.benchmarks.postgresql

import io.github.clasicrando.kdbc.core.IOUtils
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection
import kotlinx.coroutines.runBlocking
import kotlinx.io.files.Path
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
open class PgBenchmarkSuspendingCopyKdbc {
    private val connection: PgSuspendingConnection = runBlocking { getKdbcConnection() }
    private val outputPath = Path(".", "temp", "kdbc-suspending-copy-out-benchmark.csv")
    private val inputPath = Path(".", "temp", "kdbc-suspending-copy-in-benchmark.csv")

    @Setup
    open fun start() {
        IOUtils.createFileIfNotExists(outputPath)
        createBenchmarkCsv(inputPath)
        runBlocking {
            connection.createQuery(setupQuery)
                .executeClosing()
        }
    }

    @Benchmark
    open fun copyOut() = runBlocking {
        connection.copyOut(
            copyOutStatement = kdbcCopyOut,
            outputPath = outputPath,
        )
    }

    @Benchmark
    open fun copyIn() = runBlocking {
        connection.copyIn(
            copyInStatement = kdbcCopyIn,
            path = inputPath,
        )
    }

    @TearDown
    fun destroy() {
        IOUtils.deleteCatching(path = outputPath, mustExist = false)
        IOUtils.deleteCatching(path = inputPath, mustExist = false)
        runBlocking {
            if (connection.isConnected) {
                try {
                    connection.close()
                } catch (ex: Throwable) {
                    ex.printStackTrace()
                }
            }
        }
    }
}
