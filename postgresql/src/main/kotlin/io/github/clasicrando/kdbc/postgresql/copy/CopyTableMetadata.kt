package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.copy.CopyTableMetadata.Companion.QUERY
import io.github.clasicrando.kdbc.postgresql.type.PgType

/**
 * Query class for collecting table metadata before executing a `COPY FROM` operation that targets
 * a table. The fields in this class are the columns of the [QUERY] included. The companion object
 * includes the [RowParser] implementation to collect a [List] of [CopyTableMetadata] as the
 * metadata fetched.
 */
internal data class CopyTableMetadata(
    val tableOid: Int,
    val columnName: String,
    val columnOrder: Short,
    val type: PgType,
    val columnLength: Short,
) {
    companion object : RowParser<CopyTableMetadata> {
        internal const val QUERY = """
            SELECT
                c.oid table_oid, attname AS column_name, attnum column_order, atttypid AS type_oid,
                attlen AS column_length
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE
                c.relname = $1
                AND n.nspname = $2
                AND a.attnum > 0
        """

        override fun fromRow(row: DataRow): CopyTableMetadata {
            return CopyTableMetadata(
                tableOid = row.getAsNonNull("table_oid"),
                columnName = row.getAsNonNull("column_name"),
                columnOrder = row.getAsNonNull("column_order"),
                type = PgType.fromOid(row.getAsNonNull("type_oid")),
                columnLength = row.getAsNonNull("column_length"),
            )
        }

        /**
         * Convert the [metadata] supplied as the table's entire column descriptions into a [List]
         * of [PgColumnDescription]. Uses the [copyFormat] provided for every column to indicate
         * the data format.
         */
        fun getFields(
            copyFormat: CopyFormat,
            metadata: List<CopyTableMetadata>,
        ): List<PgColumnDescription> {
            return metadata
                .map {
                    PgColumnDescription(
                        fieldName = it.columnName,
                        tableOid = it.tableOid,
                        columnAttribute = it.columnOrder,
                        pgType = it.type,
                        dataTypeSize = it.columnLength,
                        typeModifier = 0,
                        formatCode = copyFormat.formatCode.toShort(),
                    )
                }
        }
    }
}
