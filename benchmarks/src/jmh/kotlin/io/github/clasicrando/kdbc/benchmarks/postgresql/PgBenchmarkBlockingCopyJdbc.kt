package io.github.clasicrando.kdbc.benchmarks.postgresql

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Level
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.TearDown
import org.openjdk.jmh.annotations.Warmup
import org.postgresql.jdbc.PgConnection
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class PgBenchmarkBlockingCopyJdbc {
    private val connection: PgConnection = getJdbcConnection()
    private val outputPath = Path.of(".", "temp", "jdbc-blocking-copy-out-benchmark.csv")
    private val inputPath = Path.of(".", "temp", "jdbc-blocking-copy-in-benchmark.csv")

    @Setup
    open fun start() {
        if (Files.notExists(outputPath)) {
            Files.createFile(outputPath)
        }
        createBenchmarkCsv(inputPath)
        getJdbcConnection().use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(copyOutSetupQuery)
                statement.execute(copyInSetupQuery)
            }
        }
    }

    @Benchmark
    open fun copyOut() {
        Files.newOutputStream(outputPath).use { stream ->
            connection.copyAPI.copyOut(jdbcCopyOut, stream)
        }
    }

    @TearDown(Level.Invocation)
    open fun cleanUp() {
        connection.execSQLUpdate("TRUNCATE TABLE public.copy_in_posts")
    }

    @Benchmark
    open fun copyIn() {
        Files.newInputStream(inputPath).use { stream ->
            connection.copyAPI.copyIn(jdbcCopyIn, stream)
        }
    }

    @TearDown
    open fun tearDown() {
        Files.deleteIfExists(outputPath)
        Files.deleteIfExists(inputPath)
        connection.close()
    }
}
