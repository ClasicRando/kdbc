package io.github.clasicrando.kdbc.postgresql.copy

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import io.github.clasicrando.kdbc.core.buffer.ByteListWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgSuspendingConnection
import io.github.clasicrando.kdbc.postgresql.result.PgDataRow
import io.github.clasicrando.kdbc.postgresql.result.PgResultSet
import kotlinx.coroutines.flow.Flow
import java.io.ByteArrayInputStream
import java.io.InputStream

internal class CopyOutCollector(
    private val copyOutStatement: CopyStatement,
    private val fields: List<PgColumnDescription>,
) {
    private suspend fun Flow<ByteArray>.toDelimitedInputStream(): InputStream {
        val buffer = ByteListWriteBuffer()
        this.collect { buffer.writeBytes(it) }
        return ByteArrayInputStream(buffer.copyToArray())
    }

    suspend fun collectSuspending(
        connection: PgSuspendingConnection,
        flow: Flow<ByteArray>,
    ): QueryResult {
        var rowCount = 0L
        val resultSet = PgResultSet(connection.typeCache, fields)

        when (copyOutStatement) {
            is CopyStatement.CopyText -> {
                val inputStream = flow.toDelimitedInputStream()
                rowCount = readTextData(
                    inputStream = inputStream,
                    resultSet = resultSet,
                    fields = fields,
                    typeCache = connection.typeCache,
                    delimiter = copyOutStatement.delimiter,
                    quote = (copyOutStatement as? CopyStatement.CopyCsv)?.quote,
                    escape = (copyOutStatement as? CopyStatement.CopyCsv)?.escape,
                )
            }
            else -> flow.collect { row ->
                val buffer = getBinaryBuffer(rowCount = ++rowCount, row = row)
                if (buffer.readShort().toInt() == -1) {
                    return@collect
                }
                buffer.reset()

                resultSet.addRow(buffer)
            }
        }

        return QueryResult(rowCount, "COPY $rowCount", resultSet)
    }

    private fun Sequence<ByteArray>.asDelimitedInputStream(): InputStream {
        return object : InputStream() {
            private val iter = this@asDelimitedInputStream.iterator()
            private var currentRow = if (iter.hasNext()) iter.next() else ByteArray(0)
            private var rowPointer = 0

            override fun read(): Int {
                if (currentRow.isEmpty()) {
                    return -1
                }
                if (rowPointer == currentRow.size) {
                    if (!iter.hasNext()) {
                        return -1
                    }
                    currentRow = iter.next()
                    rowPointer = 0
                }

                val byte = currentRow[rowPointer++]
                return byte.toInt()
            }
        }
    }

    fun collectBlocking(
        connection: PgBlockingConnection,
        sequence: Sequence<ByteArray>,
    ): QueryResult {
        var rowCount = 0L
        val resultSet = PgResultSet(connection.typeCache, fields)

        when (copyOutStatement) {
            is CopyStatement.CopyText -> {
                val inputStream = sequence.asDelimitedInputStream()
                rowCount = readTextData(
                    inputStream = inputStream,
                    resultSet = resultSet,
                    fields = fields,
                    typeCache = connection.typeCache,
                    delimiter = copyOutStatement.delimiter,
                    quote = (copyOutStatement as? CopyStatement.CopyCsv)?.quote,
                    escape = (copyOutStatement as? CopyStatement.CopyCsv)?.escape,
                )
            }
            else -> {
                for (row in sequence) {
                    val buffer = getBinaryBuffer(rowCount = ++rowCount, row = row)
                    if (buffer.readShort().toInt() == -1) {
                        continue
                    }
                    buffer.reset()

                    resultSet.addRow(buffer)
                }
            }
        }

        return QueryResult(rowCount, "COPY $rowCount", resultSet)
    }

    private fun getBinaryBuffer(rowCount: Long, row: ByteArray): ByteReadBuffer {
        return when {
            rowCount == 1L -> ByteReadBuffer(row.copyOfRange(fromIndex = 19, toIndex = row.size))
            else -> ByteReadBuffer(row)
        }
    }

    private fun readTextData(
        inputStream: InputStream,
        resultSet: PgResultSet,
        fields: List<PgColumnDescription>,
        typeCache: PgTypeCache,
        delimiter: Char,
        quote: Char? = null,
        escape: Char? = null,
    ): Long {
        var rowCount = 0L
        csvReader {
            this.delimiter = delimiter
            this.quoteChar = quote ?: '\u0000'
            this.escapeChar = escape ?: '\u0000'
        }.open(inputStream) {
            this.readAllAsSequence().forEach { row ->
                rowCount++
                val dataRow = PgDataRow(
                    rowBuffer = null,
                    pgValues = Array(row.size) { i ->
                        val rowData = row[i]
                        val fieldData = fields[i]
                        PgValue.Text(rowData, fieldData)
                    },
                    columnMapping = fields,
                    customTypeDescriptionCache = typeCache,
                )
                resultSet.addRow(dataRow)
            }
        }
        return rowCount
    }
}
