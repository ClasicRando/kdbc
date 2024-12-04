package io.github.clasicrando.kdbc.core.statement

import io.github.clasicrando.kdbc.core.chunked
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlinx.io.writeString

/**
 * Implementors of this interface can supply its data fields as row fields for a CSV or text based
 * copy operation. The values within the [List] must be ordered to match the table that is the copy
 * target.
 */
public interface CsvDataRow {
    /**
     * The values passed to a CSV/text bulk insert operation. These values will have [Any.toString]
     * to resolve as a CSV/text value. This property should be calculated once per class instance to
     * avoid [List] allocations for each call to this method. If you are not sure if this property
     * will be called, you can also delegate the field value using a [lazy] delegate.
     */
    public val values: List<Any?>
}

/**
 * Convert a [Flow] of [CsvDataRow] into a [Flow] of [ByteArray] by chunking the original flow and
 * mapping each chunk into a single buffer of the CSV data from that chunk.
 */
public fun Flow<CsvDataRow>.mapIntoCsvByteArrayChunks(chunkSize: Int): Flow<ByteArray> {
    return this.chunked(size = chunkSize).map { chunk ->
        val tempBuffer = Buffer()
        for (row in chunk) {
            val values = row.values
            for (i in values.indices) {
                if (i > 0) {
                    tempBuffer.writeString(",")
                }
                val value = values[i]?.toString() ?: ""
                if (value.any { ch -> ch == '"' || ch == ',' || ch == '\n' || ch == '\r' }) {
                    tempBuffer.writeString("\"")
                    tempBuffer.writeString(value.replace("\"", "\"\""))
                    tempBuffer.writeString("\"")
                } else {
                    tempBuffer.writeString(value)
                }
            }
            tempBuffer.writeString("\n")
        }
        tempBuffer.readByteArray()
    }
}
