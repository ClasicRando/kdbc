package io.github.clasicrando.kdbc.postgresql.connection

import com.github.doyaaaaaken.kotlincsv.dsl.csvWriter
import io.github.clasicrando.kdbc.core.IOUtils
import kotlinx.io.asOutputStream
import kotlinx.io.buffered
import kotlinx.io.files.Path

fun createTempCsvForCopy(rowCount: Int): Path {
    val outputFile = Path(".", "temp", "blocking-copy-in.csv")
    try {
        IOUtils.createFileIfNotExists(path = outputFile)
        csvWriter {
            lineTerminator = "\n"
        }.open(IOUtils.sink(path = outputFile).buffered().asOutputStream()) {
            writeRows(rows = (1..rowCount).asSequence().map { i ->
                listOf(i.toString(), "$i Value")
            })
        }
        return outputFile
    } catch (ex: Exception) {
        IOUtils.deleteCatching(path = outputFile, mustExist = false)
        throw ex
    }
}
