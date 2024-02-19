package com.github.clasicrando.common.result

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.column.ColumnData

/**
 * Collection of data as the data resulting from a query. The underlining structure of the
 * [ResultSet] is not strictly enforced. It just must conform to a structure that is [Iterable],
 * yielding [DataRow] instances.
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
interface ResultSet : Iterable<DataRow>, AutoRelease {
    /** Number of columns found within each [DataRow] entry */
    val columnCount: Int

    /** Returns the [ColumnData] for the specified column [index] */
    fun columnType(index: Int): ColumnData

    companion object
}

/** Empty [ResultSet] containing no columns and yields no rows upon iteration */
val ResultSet.Companion.EMPTY_RESULT get() = object : ResultSet {
    val rows = emptyList<DataRow>()
    override val columnCount: Int = 0
    override fun columnType(index: Int): ColumnData {
        error("Empty ResultSet should never have the columnType checked")
    }

    override fun iterator(): Iterator<DataRow> = rows.iterator()

    override fun release() {}
}
