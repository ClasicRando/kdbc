package io.github.clasicrando.kdbc.core.statement

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

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
 * Convert a [Flow] of [CsvDataRow] into a [Flow] of [ByteArray] by mapping each row into a single
 * [ByteArray] of CSV data from that row.
 */
public fun Flow<CsvDataRow>.mapIntoCsvByteArrayChunks(): Flow<ByteArray> {
    return flow {
        this@mapIntoCsvByteArrayChunks.collect { row ->
            val csvRow = row.values.joinToString(separator = ",", postfix = "\n") {
                val value = it?.toString() ?: ""
                if (value.any { ch -> ch == '"' || ch == ',' || ch == '\n' || ch == '\r' }) {
                    "\"${value.replace("\"", "\"\"")}\""
                } else {
                    value
                }
            }
            emit(csvRow.toByteArray())
        }
    }
}
