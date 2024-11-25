package io.github.clasicrando.kdbc.core.column

/**
 * Database specified descriptor of column data types. Used for decoding values in server sent
 * result rows.
 */
public interface ColumnMetadata {
    /** Name of the field for this column */
    public val fieldName: String

    /** Database specific name of the type */
    public val typeName: String

    /** ID associated with the type */
    public val dataType: Int
}
