package io.github.clasicrando.kdbc.core.result

import io.github.clasicrando.kdbc.core.column.ColumnData

/**
 * Default implementation of a [ResultSet] where [DataRow] instances can be added to the
 * [ResultSet] to allow for packing of result rows as they are received. This implementation is
 * therefore backed by a [MutableList] with a [columnMapping] [List] to provide column metadata.
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
abstract class AbstractMutableResultSet<R : DataRow, C : ColumnData>(
    val columnMapping: List<C>,
) : ResultSet {
    private var backingList: MutableList<R>? = ArrayList()

    /** Add a new [row] to the end of this [ResultSet] */
    fun addRow(row: R) {
        backingList?.add(row)
    }

    override val columnCount: Int get() = columnMapping.size

    override fun columnType(index: Int): ColumnData {
        require(index >= 0 && index < columnMapping.size) {
            "Specified index, $index is not in row. Row size = ${columnMapping.size}"
        }
        return columnMapping[index]
    }

    override fun iterator(): Iterator<DataRow> = backingList?.iterator()
        ?: error("Attempted to iterate on a closed/released ResultSet")

    /**
     * Remove all elements of the backing [MutableList] of this [ResultSet]. If the list is still
     * populated, each row will be released as well.
     */
    override fun release() {
        backingList?.let {
            for (item in it) {
                item.release()
            }
        }
        backingList = null
    }
}
