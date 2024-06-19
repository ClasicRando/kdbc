package com.github.kdbc.benchmarks.postgresql

import io.github.clasicrando.kdbc.core.connection.BlockingConnection
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.core.query.fetchAll
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
open class PgBenchmarkBlockingSingleKdbc {
    private var id = 0
    private val connection: BlockingConnection = getKdbcBlockingConnection()

    @Setup
    open fun start() {
        connection.createQuery(setupQuery)
            .executeClosing()
    }

    private fun singleStep(): Int {
        id++
        if (id > 5000) id = 1
        return id
    }

    private fun multiStep() {
        id += 10
        if (id >= 5000) id = 1
    }

    @Benchmark
    open fun querySingleRow() {
        singleStep()
        connection.createPreparedQuery(kdbcQuerySingle)
            .bind(id)
            .fetchAll(PostDataClassRowParser)
    }

    @Benchmark
    open fun queryMultipleRows() {
        multiStep()
        connection.createPreparedQuery(kdbcQuery)
            .bind(id)
            .bind(id + 10)
            .fetchAll(PostDataClassRowParser)
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
