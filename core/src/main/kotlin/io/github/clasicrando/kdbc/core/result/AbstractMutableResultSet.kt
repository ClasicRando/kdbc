package io.github.clasicrando.kdbc.core.result

import io.github.clasicrando.kdbc.core.column.ColumnMetadata

/**
 * Default implementation of a [ResultSet] where [DataRow] instances can be added to the [ResultSet]
 * to allow for packing of result rows as they are received. This implementation is therefore backed
 * by a [MutableList] with a [columnMapping] [List] to provide column metadata.
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
public abstract class AbstractMutableResultSet<R : DataRow, C : ColumnMetadata>(
    public val columnMapping: List<C>
) : ResultSet {
    final override val rowCount: Int
        get() = backingList.size

    private var backingList: MutableList<R> = ArrayList()

    final override fun get(index: Int): R {
        require(index in 0..<rowCount) {
            "Specified index, $index is not in row. Row size = ${columnMapping.size}"
        }
        return backingList[index]
    }

    /** Add a new [row] to the end of this [ResultSet] */
    public fun addRow(row: R) {
        backingList.add(row)
    }

    override val columnCount: Int
        get() = columnMapping.size

    override fun columnType(index: Int): ColumnMetadata {
        require(index >= 0 && index < columnMapping.size) {
            "Specified index, $index is not in row. Row size = ${columnMapping.size}"
        }
        return columnMapping[index]
    }

    override fun iterator(): Iterator<DataRow> = backingList.iterator()
}
