package com.github.clasicrando.common.result

import com.github.clasicrando.common.column.ColumnData

/**
 * Collection of data as the data resulting from a query. The underlining structure of the
 * [ResultSet] is not strictly enforced. It just must conform to a structure that is [Iterable],
 * yielding [DataRow] instances.
 */
interface ResultSet : Iterator<DataRow>, Iterable<DataRow> {
    override fun iterator(): Iterator<DataRow> = this

    /** Number of columns found within each [DataRow] entry */
    val columnCount: Int

    /** Returns the [ColumnData] for the specified column [index] */
    fun columnType(index: Int): ColumnData

    companion object
}

/** Empty [ResultSet] containing no columns and yields no rows upon iteration */
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
