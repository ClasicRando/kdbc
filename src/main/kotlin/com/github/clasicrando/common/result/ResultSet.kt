package com.github.clasicrando.common.result

import com.github.clasicrando.common.column.ColumnData

interface ResultSet : Iterator<DataRow>, Iterable<DataRow> {
    override fun iterator(): Iterator<DataRow> = this

    val columnCount: Int
    fun columnType(index: Int): ColumnData

    companion object
}

val ResultSet.Companion.EMPTY_RESULT get() = object : ResultSet {
    override val columnCount: Int = 0
    override fun columnType(index: Int): ColumnData {
        error("Empty ResultSet should never have the columnType checked")
    }

    override fun hasNext(): Boolean = false

    override fun next(): DataRow {
        error("Empty ResultSet should never have to return a next element")
    }
}
