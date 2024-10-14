package io.github.clasicrando.kdbc.benchmarks.postgresql

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.pool.PgAsyncConnectionPool
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
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit

@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class PgBenchmarkAsyncMultiKdbc {
    private var id = 0
    private val pool = PgAsyncConnectionPool(
        connectOptions = kdbcConnectOptions,
        poolOptions = poolOptions,
    )

    @Setup
    open fun start(): Unit = runBlocking {
        pool.acquire().use {
            it.createQuery(setupQuery).executeClosing()
        }
    }

    private fun step(): Int {
        id++
        if (id > 5000) id = 1
        return id
    }

    private suspend fun executeQuery(stepId: Int): List<PostDataClass> {
        return pool.acquire().use { conn ->
            conn.createPreparedQuery(kdbcQuerySingle)
                .bind(stepId)
                .fetchAll(PostDataClassRowParser)
        }
    }

    @Benchmark
    open fun querySingleRow() = runBlocking {
        val results = List(concurrencyLimit) {
            val stepId = step()
            async { executeQuery(stepId) }
        }
        results.awaitAll()
    }
}
