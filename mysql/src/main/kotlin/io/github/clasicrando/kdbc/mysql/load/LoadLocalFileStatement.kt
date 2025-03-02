package io.github.clasicrando.kdbc.mysql.load

/**
 * Constructor of a valid `LOAD DATA LOCAL` statement. See the
 * [mysql docs](https://dev.mysql.com/doc/refman/9.0/en/load-data.html) for more details.
 */
public data class LoadLocalFileStatement(
    /** Table to load into. If the table is in another database use the [databaseName] property */
    val targetTable: String,
    /** Optional database name to qualify the target table */
    val databaseName: String? = null,
    /** Optional specification for how PK/Unique constraints are handled during the load */
    val duplicateHandling: DuplicateHandling = DuplicateHandling.NONE,
    val characterSet: String = "utf8mb4",
    val delimiter: Char = ',',
    val quoteChar: Char = '"',
    val newline: String = "\n",
    val skipLines: Int = 1,
    val columns: List<String> = emptyList(),
) {
    public enum class DuplicateHandling {
        /**
         * Specify that when the imported data contains a conflict with a PK/Unique, the incoming
         * value will replace the existing value. Additionally, any error during data parsing will
         * result in an aborted operation.
         */
        REPLACE,
        /**
         * Specify that when the imported data contains a conflict with a PK/Unique, the incoming
         * value is discarded. Additionally, any error during data parsing will be treated as a
         * warning and the incoming record discarded.
         */
        IGNORE,
        /**
         * Specify neither the `REPLACE`/`IGNORE` option. This means that PK/Unique conflicts as
         * well as data parsing error will abort the current operation.
         */
        NONE,
    }

    /** Build the query string to execute a `LOAD DATA LOCAL` command using the provided values. */
    public fun toQuery(): String {
        return buildString {
            append("LOAD DATA LOCAL INFILE 'placeholder'")
            if (duplicateHandling != DuplicateHandling.NONE) {
                append(' ')
                append(duplicateHandling.name)
            }
            append(" INTO TABLE ")
            if (databaseName != null) {
                append(databaseName.escape())
                append('.')
            }
            append(targetTable.escape())
            append(" CHARACTER SET ")
            append(characterSet.escape())
            append(" FIELDS TERMINATED BY '")
            append(delimiter)
            append("' ENCLOSED BY '")
            append(quoteChar)
            append("' ESCAPED BY '' LINES TERMINATED BY '")
            append(newline)
            append("' STARTING BY ''")
            if (skipLines > 0) {
                append(" IGNORE ")
                append(skipLines)
                append(" LINES")
            }
            if (columns.isNotEmpty()) {
                columns.joinTo(this, separator = ",", prefix = " (", postfix = ")") { it.escape() }
            }
        }
    }

    private fun String.escape(): String {
        return "`${this.replace("`", "``")}`"
    }
}
