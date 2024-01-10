package com.github.clasicrando.common.result

import com.github.clasicrando.common.column.ColumnInfo

/**
 * Default implementation of a [ResultSet] where [DataRow] instances can be added to the
 * [ResultSet] to allow for packing of result rows as they are received. This implementation is
 * therefore backed by a [MutableList] with a [columnMapping] [List] to provide column metadata.
 */
internal class MutableResultSet(internal val columnMapping: List<ColumnInfo>) : ResultSet {
    private var rowIndex = 0
    private val backingList = mutableListOf<DataRow>()

    /** [Map] of column name to column index. Used within other internal classes */
    internal val columnMap = columnMapping.withIndex()
        .associate { (i, value) ->
            value.name to i
        }

    /** Add a new [row] to the end of this [ResultSet] */
    fun addRow(row: DataRow) {
        backingList.add(row)
    }

    override val columnCount: Int get() = columnMapping.size

    override fun columnType(index: Int): ColumnInfo {
        require(index >= 0 && index < columnMapping.size) {
            "Specified index, $index is not in row. Row size = ${columnMapping.size}"
        }
        return columnMapping[index]
    }

    override fun hasNext(): Boolean = rowIndex < backingList.size

    override fun next(): DataRow {
        val temp = backingList[rowIndex]
        rowIndex++
        return temp
    }
}