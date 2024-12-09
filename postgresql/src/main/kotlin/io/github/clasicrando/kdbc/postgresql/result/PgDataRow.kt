package io.github.clasicrando.kdbc.postgresql.result

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.ensureNonNull
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgFormatCode
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.exceptions.PgException
import io.github.clasicrando.kdbc.postgresql.type.PgType
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription
import kotlin.reflect.KType

/** Postgresql specific implementation for a [DataRow] */
internal class PgDataRow(
    private var pgValues: Array<PgValue?>,
    private val columnMapping: List<PgColumnDescription>,
    private val typeCache: PgTypeCache,
) : DataRow {
    /**
     * Check to ensure the [index] is valid for this row
     *
     * @throws IllegalArgumentException if the [index] can not be found in the [columnMapping]
     */
    private fun checkIndex(index: Int) {
        require(index in columnMapping.indices) {
            val range = columnMapping.indices
            "Index $index is not a valid index in this result. Values must be in $range"
        }
    }

    private fun getPgType(index: Int): PgType {
        checkIndex(index)
        return columnMapping[index].pgType
    }

    private fun <T : Any> decode(index: Int, deserializer: PgTypeDescription<T>): T? {
        val pgValue = pgValues[index] ?: return null
        return deserializer.decode(pgValue)
    }

    override fun indexFromColumn(column: String): Int {
        val result = columnMapping.indexOfFirst { c -> c.fieldName == column }
        if (result >= 0) {
            return result
        }
        val columns = columnMapping.withIndex().joinToString { (i, c) -> "$i->${c.fieldName}" }
        error("Could not find column in mapping. Column = '$column', columns = $columns")
    }

    override fun get(index: Int, type: KType): Any? {
        val pgType = getPgType(index)
        val nonNullType = type.ensureNonNull()
        val typeDescription =
            typeCache.getTypeDescription<Any>(nonNullType)
                ?: throw PgException("Could not find type description for $nonNullType")
        if (typeDescription.dbType.oid == pgType.oid) {
            return decode(index, typeDescription)
        }
        val pgValue = pgValues[index] ?: return null
        checkOrColumnDecodeError(
            check = typeDescription.isCompatible(pgType),
            kType = nonNullType,
            type = pgValue.typeData,
        ) {
            "Actual column type is not compatible with required type"
        }
        return decode(index, typeDescription)
    }

    internal companion object {
        fun fromBuffer(
            buffer: ByteReadBuffer,
            columnMapping: List<PgColumnDescription>,
            typeCache: PgTypeCache,
        ): PgDataRow {
            val count = buffer.readShort()
            val pgValues =
                Array(count.toInt()) {
                    val length = buffer.readInt()
                    if (length < 0) {
                        return@Array null
                    }
                    val columnType = columnMapping[it]
                    when (columnType.formatCode) {
                        PgFormatCode.Text -> PgValue.Text(buffer.slice(length), columnType)
                        PgFormatCode.Binary -> PgValue.Binary(buffer.slice(length), columnType)
                    }
                }

            return PgDataRow(
                pgValues = pgValues,
                columnMapping = columnMapping,
                typeCache = typeCache,
            )
        }
    }
}
