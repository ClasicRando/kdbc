package io.github.clasicrando.kdbc.postgresql.copy

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import io.github.clasicrando.kdbc.core.buffer.ByteListWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.result.PgDataRow
import io.github.clasicrando.kdbc.postgresql.result.PgResultSet
import kotlinx.coroutines.flow.Flow
import java.io.ByteArrayInputStream
import java.io.InputStream

/**
 * Collector object that converts text and binary `COPY TO` data streams into a single
 * [QueryResult].
 */
internal class CopyOutCollector(
    private val copyOutStatement: CopyStatement,
    private val fields: List<PgColumnDescription>,
) {
    /**
     * Convert the [Flow] of [ByteArray] into an [InputStream] for text file parsing. This collects
     * the flow contents into a [ByteListWriteBuffer] and collects a [ByteArray] into a
     * [ByteArrayInputStream].
     */
    private suspend fun Flow<ByteArray>.toDelimitedInputStream(): InputStream {
        val buffer = ByteListWriteBuffer()
        this.collect { buffer.writeBytes(it) }
        return ByteArrayInputStream(buffer.copyToArray())
    }

    /**
     * Collect the [Flow] of [ByteArray] into a single [QueryResult]. If the [copyOutStatement]
     * is a text based copy statement, data is decoded using the text format. Otherwise, the data
     * is decoded as if it were the binary protocol.
     *
     * For text formatted data, [readTextData] is called, whereas for binary data, each [ByteArray]
     * is passed into [getBinaryBuffer] to get a [ByteReadBuffer] as the next row's buffer. There
     * is a special case for binary data where if the first short value extracted from the buffer
     * is -1, that is an indication the copy data is done and that row should be discarded.
     */
    suspend fun collectAsync(
        connection: PgAsyncConnection,
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
                    header = copyOutStatement.header,
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

    /**
     * [InputStream] implementation backed by a [Sequence] of [ByteArray]. The [Sequence] is
     * iterated over and each [ByteArray] is used to supply the [read] method until the iterator
     * is exhausted and the final [ByteArray] is consumed.
     */
    inner class ByteArraySequenceInputStream(sequence: Sequence<ByteArray>) : InputStream() {
        private val iter = sequence.iterator()
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

    /**
     * Collect the [Sequence] of [ByteArray] into a single [QueryResult]. If the [copyOutStatement]
     * is a text based copy statement, data is decoded using the text format. Otherwise, the data
     * is decoded as if it were the binary protocol.
     *
     * For text formatted data, [readTextData] is called, whereas for binary data, each [ByteArray]
     * is passed into [getBinaryBuffer] to get a [ByteReadBuffer] as the next row's buffer. There
     * is a special case for binary data where if the first short value extracted from the buffer
     * is -1, that is an indication the copy data is done and that row should be discarded.
     */
    fun collectBlocking(
        connection: PgBlockingConnection,
        sequence: Sequence<ByteArray>,
    ): QueryResult {
        var rowCount = 0L
        val resultSet = PgResultSet(connection.typeCache, fields)

        when (copyOutStatement) {
            is CopyStatement.CopyText -> {
                rowCount = readTextData(
                    inputStream = ByteArraySequenceInputStream(sequence),
                    resultSet = resultSet,
                    fields = fields,
                    typeCache = connection.typeCache,
                    delimiter = copyOutStatement.delimiter,
                    quote = (copyOutStatement as? CopyStatement.CopyCsv)?.quote,
                    escape = (copyOutStatement as? CopyStatement.CopyCsv)?.escape,
                    header = copyOutStatement.header,
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

    /**
     * Put the [row] data into a [ByteReadBuffer]. Has a special case where for the first row, the
     * first 19 bytes should be ignored since they are the binary copy's file header.
     */
    private fun getBinaryBuffer(rowCount: Long, row: ByteArray): ByteReadBuffer {
        return when {
            rowCount == 1L -> ByteReadBuffer(row.copyOfRange(fromIndex = 19, toIndex = row.size))
            else -> ByteReadBuffer(row)
        }
    }

    /**
     * Uses the [inputStream], [fields] and text data config details to parse and populate the
     * [resultSet] with rows.
     */
    private fun readTextData(
        inputStream: InputStream,
        resultSet: PgResultSet,
        fields: List<PgColumnDescription>,
        typeCache: PgTypeCache,
        delimiter: Char,
        quote: Char?,
        escape: Char?,
        header: CopyHeader?,
    ): Long {
        var rowCount = 0L
        csvReader {
            this.delimiter = delimiter
            this.quoteChar = quote ?: '\u0000'
            this.escapeChar = escape ?: '\u0000'
        }.open(inputStream) {
            val skip = if (header != null && header != CopyHeader.False) 1 else 0
            this.readAllAsSequence().drop(skip).forEach { row ->
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
