package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.column.ColumnData

/**
 * Postgresql specified implementation of [ColumnData]. Provides the required fields using data
 * provided from row description messages sent from the postgres backend. Other fields as also
 * included that related to postgres specific properties.
 */
data class PgColumnDescription(
    override val fieldName: String,
    /** OID of the table this field. If the field is not part of a table, the value is 0. */
    val tableOid: Int,
    /** Attribute number of the field. If the field is not part of a table, the value is 0. */
    val columnAttribute: Short,
    /** OID of the field's data type */
    val pgType: PgType,
    /** Size of the data type. Negative values denote a variable width type. */
    val dataTypeSize: Short,
    /**
     * Modifier of the data type (see
     * [pg_attribute](https://www.postgresql.org/docs/current/catalog-pg-attribute.html)). Will be
     * -1 when the type does not need `atttypmod`.
     */
    val typeModifier: Int,
    /** Format code of the field. Currently, this value will be either 0 (text) or 1 (binary). */
    val formatCode: Short,
) : ColumnData {
    override val dataType: Int get() = pgType.oid
    override val typeName: String = fieldName
    override val typeSize: Long = dataTypeSize.toLong()

    companion object {
        fun dummyDescription(pgType: PgType, formatCode: Short): PgColumnDescription {
            return PgColumnDescription(
                fieldName = "",
                tableOid = 0,
                columnAttribute = 0,
                pgType = pgType,
                dataTypeSize = 0,
                typeModifier = 0,
                formatCode = formatCode,
            )
        }
    }
}
