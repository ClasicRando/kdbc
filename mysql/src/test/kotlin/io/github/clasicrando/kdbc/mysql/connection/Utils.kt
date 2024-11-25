package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.Either
import io.github.clasicrando.kdbc.core.result.QueryResult
import kotlinx.coroutines.flow.Flow

suspend fun Flow<Either<QueryResult, DataRow>>.collectResults(): Pair<List<QueryResult>, List<List<DataRow>>> {
    val results = mutableListOf<QueryResult>()
    val rows = mutableListOf(mutableListOf<DataRow>())
    this.collect {
        when (it) {
            is Either.Left -> {
                results.add(it.inner)
                rows.add(mutableListOf())
            }
            is Either.Right -> {
                rows[results.size].add(it.inner)
            }
        }
    }
    while (rows.size > results.size) {
        rows.removeLast()
    }
    return results to rows
}
