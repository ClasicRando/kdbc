package com.github.kdbc.benchmarks.postgresql

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
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
@Measurement(iterations = 20, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
open class JdbcBenchmarkConcurrentSingle {
    private var id = 0
    private val dataSource = getJdbcDataSource()

    @Setup
    open fun start() {
        dataSource.connection.use { connection ->
            connection.createStatement().use { statement ->
                statement.execute(setupQuery)
            }
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
                dataSource.connection.use {
                    it.prepareStatement(jdbcQuerySingle).use { preparedStatement ->
                        preparedStatement.setInt(1, stepId)
                        preparedStatement.executeQuery().use { resultSet ->
                            val items = mutableListOf<PostDataClass>()
                            while (resultSet.next()) {
                                val item = PostDataClass(
                                    resultSet.getInt(1),
                                    resultSet.getString(2),
                                    resultSet.getObject(3, java.time.LocalDateTime::class.java).toKotlinLocalDateTime(),
                                    resultSet.getObject(4, java.time.LocalDateTime::class.java).toKotlinLocalDateTime(),
                                    resultSet.getInt(5),
                                    resultSet.getInt(6),
                                    resultSet.getInt(7),
                                    resultSet.getInt(8),
                                    resultSet.getInt(9),
                                    resultSet.getInt(10),
                                    resultSet.getInt(11),
                                    resultSet.getInt(12),
                                    resultSet.getInt(13),
                                )
                                items.add(item)
                            }
                        }
                    }
                }
            }
        }
        result.awaitAll()
    }

    @TearDown
    fun destroy() {
        dataSource.close()
    }
}