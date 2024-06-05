package io.github.clasicrando.kdbc.postgresql.connection

import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import io.github.clasicrando.kdbc.core.IoUtils
import kotlinx.io.asOutputStream
import kotlinx.io.buffered
import kotlinx.io.files.Path

fun createTempCsvForCopy(rowCount: Int): Path {
    val outputFile = Path(".", "temp", "blocking-copy-in.csv")
    try {
        IoUtils.createIfNotExists(path = outputFile)
        csvWriter {
            lineTerminator = "\n"
        }.open(IoUtils.sink(path = outputFile).buffered().asOutputStream()) {
            writeRows(rows = (1..rowCount).asSequence().map { i ->
                listOf(i.toString(), "$i Value")
            })
        }
        return outputFile
    } catch (ex: Exception) {
        IoUtils.deleteCatching(path = outputFile, mustExist = false)
        throw ex
    }
}
