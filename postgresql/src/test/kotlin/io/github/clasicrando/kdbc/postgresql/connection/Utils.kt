package io.github.clasicrando.kdbc.postgresql.connection

import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.Either
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.postgresql.IOUtils
import kotlinx.coroutines.flow.Flow
import kotlinx.io.asOutputStream
import kotlinx.io.buffered
import kotlinx.io.files.Path

fun createTempCsvForCopy(rowCount: Int): Path {
    val outputFile = Path(".", "temp", "blocking-copy-in.csv")
    try {
        IOUtils.createFileIfNotExists(path = outputFile)
        csvWriter { lineTerminator = "\n" }
            .open(IOUtils.sink(path = outputFile).buffered().asOutputStream()) {
                writeRows(
                    rows = (1..rowCount).asSequence().map { i -> listOf(i.toString(), "$i Value") }
                )
            }
        return outputFile
    } catch (ex: Exception) {
        IOUtils.deleteCatching(path = outputFile, mustExist = false)
        throw ex
    }
}

suspend fun Flow<Either<QueryResult, DataRow>>.collectResults():
    Pair<List<QueryResult>, List<List<DataRow>>> {
    val results = mutableListOf<QueryResult>()
    val rows = mutableListOf<MutableList<DataRow>>()
    this.collect {
        when (it) {
            is Either.Left -> results.add(it.inner)
            is Either.Right -> {
                if (rows.size == results.size) {
                    rows.add(mutableListOf())
                }
                rows[results.size].add(it.inner)
            }
        }
    }
    return results to rows
}
