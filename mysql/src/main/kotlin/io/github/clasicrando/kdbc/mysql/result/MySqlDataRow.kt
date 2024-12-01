package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.ensureNonNull
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeCache
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeDescription
import io.github.clasicrando.kdbc.mysql.type.MysqlTypeInfo
import kotlin.reflect.KType

/**
 * [DataRow] implementation for MySQL. Stored each [MySqlValue] in the row as well as the associated
 * [MySqlColumn] info and the [MySqlTypeCache] to lookup type descriptions for decoding.
 */
internal class MySqlDataRow(
    private val values: Array<MySqlValue?>,
    private val columns: List<MySqlColumn>,
    private val typeCache: MySqlTypeCache,
) : DataRow {
    /** Quick lookup map for column names to the corresponding index */
    private val columnNames =
        columns.asSequence().mapIndexed { index, column -> column.name to index }.toMap()

    override fun indexFromColumn(column: String): Int {
        val index = columnNames[column]
        if (index != null) {
            return index
        }

        val columnCollection = columns.withIndex().joinToString { (i, c) -> "$i->${c.name}" }
        return columnNames[column]
            ?: throw MySqlException(
                "Could not find column in mapping. Column = '$column', columns = $columnCollection"
            )
    }

    override fun get(index: Int, type: KType): Any? {
        val mySqlType = getMySqlType(index)
        val nonNullType = type.ensureNonNull()
        val typeDescription = typeCache.getTypeDescription<Any>(nonNullType)
        if (typeDescription.dbType.inner == mySqlType.type.inner) {
            return decode(index, typeDescription)
        }
        checkOrColumnDecodeError(
            check = typeDescription.isCompatible(mySqlType.type),
            kType = nonNullType,
            type = columns[index],
        ) {
            "Actual column type is not compatible with required type"
        }
        return decode(index, typeDescription)
    }

    /**
     * Check to ensure the [index] is valid for this row
     *
     * @throws IllegalArgumentException if the [index] can not be found in the [columns]
     */
    private fun checkIndex(index: Int) {
        require(index in columns.indices) {
            val range = columns.indices
            "Index $index is not a valid index in this result. Values must be in $range"
        }
    }

    private fun getMySqlType(index: Int): MysqlTypeInfo {
        checkIndex(index)
        return columns[index].typeInfo
    }

    private fun <T : Any> decode(index: Int, deserializer: MySqlTypeDescription<T>): T? {
        val pgValue = values[index] ?: return null
        return deserializer.decode(pgValue)
    }

    override fun toString(): String {
        return "MySqlDataRow([${values.joinToString()}])"
    }
}
