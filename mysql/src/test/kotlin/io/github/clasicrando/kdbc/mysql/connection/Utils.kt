package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.Either
import io.github.clasicrando.kdbc.core.result.QueryResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlinx.coroutines.flow.Flow
import kotlinx.io.asSink
import kotlinx.io.buffered
import kotlinx.io.writeString

fun createTempCsvForLoad(outputFile: Path, rowCount: Long): Path {
    try {
        Files.createDirectories(outputFile.parent)
        Files.newOutputStream(outputFile, StandardOpenOption.CREATE).use { out ->
            out.asSink().buffered().use {
                (1..rowCount).forEach { i -> it.writeString("$i,$i Value\n") }
            }
        }
        return outputFile
    } catch (ex: Exception) {
        Files.deleteIfExists(outputFile)
        throw ex
    }
}

suspend fun Flow<Either<QueryResult, DataRow>>.collectResults():
    Pair<List<QueryResult>, List<List<DataRow>>> {
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
