package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgType

internal data class CopyOutTableMetadata(
    val tableOid: Int,
    val columnName: String,
    val columnOrder: Short,
    val type: PgType,
    val columnLength: Short,
) {
    companion object : RowParser<CopyOutTableMetadata> {
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

        override fun fromRow(row: DataRow): CopyOutTableMetadata {
            return CopyOutTableMetadata(
                tableOid = row.getAsNonNull("table_oid"),
                columnName = row.getAsNonNull("column_name"),
                columnOrder = row.getAsNonNull("column_order"),
                type = PgType.fromOid(row.getAsNonNull("type_oid")),
                columnLength = row.getAsNonNull("column_length"),
            )
        }

        fun getFields(
            copyFormat: CopyFormat,
            metadata: List<CopyOutTableMetadata>,
        ): List<PgColumnDescription> {
            return metadata
                .map { metadata ->
                    PgColumnDescription(
                        fieldName = metadata.columnName,
                        tableOid = metadata.tableOid,
                        columnAttribute = metadata.columnOrder,
                        pgType = metadata.type,
                        dataTypeSize = metadata.columnLength,
                        typeModifier = 0,
                        formatCode = copyFormat.formatCode.toShort(),
                    )
                }
        }
    }
}
