package io.github.clasicrando.kdbc.benchmarks.postgresql

import io.github.clasicrando.kdbc.benchmarks.IOUtils
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import kotlinx.io.buffered
import kotlinx.io.files.Path
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
import java.nio.file.Files
import java.util.concurrent.TimeUnit

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class PgBenchmarkBlockingCopyKdbc {
    private val connection: PgBlockingConnection = getKdbcBlockingConnection()
    private val outputPath = Path(".", "temp", "kdbc-blocking-copy-out-benchmark.csv")
    private val inputPath = Path(".", "temp", "kdbc-blocking-copy-in-benchmark.csv")
    private val outputPathJava = java.nio.file.Path.of(outputPath.toString())
    private val inputPathJava = java.nio.file.Path.of(inputPath.toString())

    @Setup
    open fun start() {
        IOUtils.createFileIfNotExists(outputPath)
        createBenchmarkCsv(inputPath)
        connection.createQuery(copyOutSetupQuery).executeClosing()
        connection.createQuery(copyInSetupQuery).executeClosing()
    }

    @Benchmark
    open fun copyOutSink() {
        IOUtils.sink(outputPath).buffered().use { sink ->
            connection.copyOut(
                copyOutStatement = kdbcCopyOut,
                sink = sink,
            )
        }
    }

    @Benchmark
    open fun copyOutStream() {
        Files.newOutputStream(outputPathJava).use { stream ->
            connection.copyOut(
                copyOutStatement = kdbcCopyOut,
                outputStream = stream,
            )
        }
    }

    @TearDown(Level.Invocation)
    open fun cleanUp() {
        connection.createQuery("TRUNCATE TABLE public.copy_in_posts;").executeClosing()
    }

    @Benchmark
    open fun copyInSource() {
        IOUtils.source(inputPath).buffered().use { source ->
            connection.copyIn(
                copyInStatement = kdbcCopyIn,
                source = source,
            )
        }
    }

    @Benchmark
    open fun copyInStream() {
        Files.newInputStream(inputPathJava).use { stream ->
            connection.copyIn(
                copyInStatement = kdbcCopyIn,
                inputStream = stream,
            )
        }
    }

    @TearDown
    fun destroy() {
        IOUtils.deleteCatching(path = outputPath, mustExist = false)
        IOUtils.deleteCatching(path = inputPath, mustExist = false)
        if (connection.isConnected) {
            try {
                connection.close()
            } catch (ex: Throwable) {
                ex.printStackTrace()
            }
        }
    }
}
