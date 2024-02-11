package com.github.clasicrando.common.column

/**
 * Database specified descriptor of column data types. Used for decoding values in server sent
 * result rows.
 */
interface ColumnData {
    /** Name of the field for this column */
    val fieldName: String
    /** Database specific name of the type */
    val typeName: String
    /** ID associated with the type */
    val dataType: Int
    /** Number of bytes required for the type */
    val typeSize: Long
}
