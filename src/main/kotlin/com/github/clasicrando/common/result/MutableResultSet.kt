package com.github.clasicrando.common.result

import com.github.clasicrando.common.column.ColumnData

class MutableResultSet(internal val columnMapping: List<ColumnData>) : ResultSet {
    private var rowIndex = 0
    private val backingList = mutableListOf<DataRow>()

    internal val columnMap = columnMapping.withIndex()
        .associate { (i, value) ->
            value.name to i
        }

    fun addRow(row: DataRow) {
        backingList.add(row)
    }

    override val columnCount: Int get() = columnMapping.size

    override fun columnType(index: Int): ColumnData {
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