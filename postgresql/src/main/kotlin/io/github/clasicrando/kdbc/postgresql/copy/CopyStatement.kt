package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.quoteIdentifier

/**
 * Constructor of valid COPY statements for the Postgresql extended query feature. See the
 * postgresql [docs](https://www.postgresql.org/docs/current/sql-copy.html) for more information.
 * Most of the parameter descriptions were taken from the Postgresql docs if they represent the
 * exact option or query component.
 */
sealed interface CopyStatement {
    /** Specifies the data format to be read or written. Default is [CopyFormat.Text] */
    val format: CopyFormat

    fun toQuery(): String

    interface CopyText {
        /**
         * Specifies the character that separates columns within each row of the file. The default
         * character is tab when in [CopyFormat.Text] and comma when [CopyFormat.CSV].
         */
        val delimiter: Char

        /**
         * Specifies the string that represents a null value. The default is "\N" (backslash-N) in
         * [CopyFormat.Text] format, and an unquoted empty string in [CopyFormat.CSV]. You might
         * prefer an empty string even in text format for cases where you don't want to distinguish
         * nulls from empty strings
         */
        val nullString: String

        /**
         * Specifies the string that represents a default value. Each time the string is found in
         * the input file, the default value of the corresponding column will be used. This option
         * is ignored unless the type is [From].
         */
        val default: String?

        /**
         * Specifies that the file contains a header line with the names of each column in the
         * file. On output, the first line contains the column names from the table. On input, the
         * first line is discarded when this option is set to [CopyHeader.TRUE]. If this option is
         * set to [CopyHeader.MATCH], the number and names of the columns in the header line must
         * match the actual column names of the table, in order; otherwise an error is raised. This
         * option is ignored when [CopyFormat.Binary]. The [CopyHeader.MATCH] option is only valid
         * for [From].
         */
        val header: CopyHeader?
    }

    interface CopyCsv : CopyText {
        /**
         * Specifies the quoting character to be used when a data value is quoted. The default is
         * double-quote. This must be a single one-byte character.
         */
        val quote: Char

        /**
         * Specifies the character that should appear before a data character that matches the
         * [quote] value. The default is the same as the [quote] value (so that the quoting
         * character is doubled if it appears in the data).
         */
        val escape: Char
    }

    interface CopyQuery {
        /** SQL query to be executed as the supplier of the records in the COPY TO operation */
        val query: String
    }

    interface CopyTable {
        /** Schema of the target table */
        val schemaName: String

        /** Target table of the COPY operation. Must already exist */
        val tableName: String

        /**
         * Optional [List] of column names that will be copied. If no column list is specified then
         * all columns of the table except for the generated columns will be copied.
         */
        val columnNames: List<String>
    }

    sealed interface To : CopyStatement

    sealed interface From : CopyStatement

    /**
     * [CopyStatement] implementation for copying to STDOUT as CSV data extracted from the query
     * specified.
     */
    data class QueryToCsv(
        override val query: String,
        override val delimiter: Char = ',',
        override val nullString: String = "",
        override val default: String? = null,
        override val header: CopyHeader? = null,
        override val quote: Char = '"',
        override val escape: Char = quote,
        /**
         * Forces quoting to be used for all non-NULL values in each specified column. NULL output
         * is never quoted. If [ForceAgainstColumns.All] is specified, non-NULL values will be
         * quoted in all columns. This option is allowed only when [To], and only when using
         * [CopyFormat.CSV], otherwise the value is ignored.
         */
        val forceQuote: ForceAgainstColumns? = null,
    ) : To, CopyQuery, CopyCsv {
        override val format: CopyFormat = CopyFormat.CSV

        override fun toQuery(): String =
            buildString {
                append("COPY (").append(query).append(") TO STDOUT")
                appendCsvOptions(this)
                forceQuote?.let {
                    append(", FORCE_QUOTE '").append(it).append('\'')
                }
                append(')')
            }
    }

    /**
     * [CopyStatement] implementation for copying to STDOUT as CSV data extracted from the table
     * specified.
     */
    data class TableToCsv(
        override val schemaName: String,
        override val tableName: String,
        override val columnNames: List<String> = emptyList(),
        override val delimiter: Char = ',',
        override val nullString: String = "",
        override val default: String? = null,
        override val header: CopyHeader? = null,
        override val quote: Char = '"',
        override val escape: Char = quote,
        val forceQuote: ForceAgainstColumns? = null,
    ) : To, CopyTable, CopyCsv {
        override val format: CopyFormat = CopyFormat.CSV

        override fun toQuery(): String =
            buildString {
                append("COPY ")
                    .append(schemaName.quoteIdentifier())
                    .append('.')
                    .append(tableName.quoteIdentifier())
                if (columnNames.isNotEmpty()) {
                    columnNames.joinTo(buffer = this, separator = ",", prefix = "(", postfix = ")")
                }
                append(" TO STDOUT")
                appendCsvOptions(this)
                forceQuote?.let {
                    append(", FORCE_QUOTE '").append(it).append('\'')
                }
                append(')')
            }
    }

    /**
     * [CopyStatement] implementation for copying to STDOUT as text delimited data extracted from
     * the query specified.
     */
    data class QueryToText(
        override val query: String,
        override val delimiter: Char = '\t',
        override val nullString: String = "\\N",
        override val default: String? = null,
        override val header: CopyHeader? = null,
    ) : To, CopyQuery, CopyText {
        override val format: CopyFormat = CopyFormat.Text

        override fun toQuery(): String =
            buildString {
                append("COPY (").append(query).append(") TO STDOUT")
                appendTextOptions(this)
            }
    }

    /**
     * [CopyStatement] implementation for copying to STDOUT as text delimited data extracted from
     * the table specified.
     */
    data class TableToText(
        override val schemaName: String,
        override val tableName: String,
        override val columnNames: List<String> = emptyList(),
        override val delimiter: Char = '\t',
        override val nullString: String = "\\N",
        override val default: String? = null,
        override val header: CopyHeader? = null,
    ) : To, CopyTable, CopyText {
        override val format: CopyFormat = CopyFormat.Text

        override fun toQuery(): String =
            buildString {
                append("COPY ")
                    .append(schemaName.quoteIdentifier())
                    .append('.')
                    .append(tableName.quoteIdentifier())
                if (columnNames.isNotEmpty()) {
                    columnNames.joinTo(buffer = this, separator = ",", prefix = "(", postfix = ")")
                }
                append(" TO STDOUT")
                appendTextOptions(this)
            }
    }

    /**
     * [CopyStatement] implementation for copying to STDOUT as binary data extracted from the query
     * specified.
     */
    data class QueryToBinary(override val query: String) : To, CopyQuery {
        override val format: CopyFormat = CopyFormat.Binary

        override fun toQuery(): String =
            buildString {
                append("COPY (").append(query).append(") TO STDOUT (FORMAT binary)")
            }
    }

    /**
     * [CopyStatement] implementation for copying to STDOUT as binary data extracted from the table
     * specified.
     */
    data class TableToBinary(
        override val schemaName: String,
        override val tableName: String,
        override val columnNames: List<String> = emptyList(),
    ) : To, CopyTable {
        override val format: CopyFormat = CopyFormat.Binary

        override fun toQuery(): String =
            buildString {
                append("COPY ")
                    .append(schemaName.quoteIdentifier())
                    .append('.')
                    .append(tableName.quoteIdentifier())
                if (columnNames.isNotEmpty()) {
                    columnNames.joinTo(buffer = this, separator = ",", prefix = "(", postfix = ")")
                }
                append(" TO STDOUT (FORMAT binary)")
            }
    }

    /**
     * [CopyStatement] implementation for copying from STDIN as CSV data into the table specified
     */
    data class TableFromCsv(
        override val schemaName: String,
        override val tableName: String,
        override val columnNames: List<String> = emptyList(),
        override val delimiter: Char = ',',
        override val nullString: String = "",
        override val default: String? = null,
        override val header: CopyHeader? = null,
        override val quote: Char = '"',
        override val escape: Char = quote,
        /**
         * Do not match the specified columns' values against the null string. In the default case
         * where the null string is empty, this means that empty values will be read as zero-length
         * strings rather than nulls, even when they are not quoted.
         */
        val forceNotNull: ForceAgainstColumns.Select? = null,
        /**
         * Match the specified columns' values against the null string, even if it has been quoted,
         * and if a match is found set the value to NULL. In the default case where the null string
         * is empty, this converts a quoted empty string into NULL.
         */
        val forceNull: ForceAgainstColumns.Select? = null,
    ) : From, CopyTable, CopyCsv {
        override val format: CopyFormat = CopyFormat.CSV

        override fun toQuery(): String =
            buildString {
                append("COPY ")
                    .append(schemaName.quoteIdentifier())
                    .append('.')
                    .append(tableName.quoteIdentifier())
                if (columnNames.isNotEmpty()) {
                    columnNames.joinTo(buffer = this, separator = ",", prefix = "(", postfix = ")")
                }
                append(" FROM STDIN")
                appendCsvOptions(this)
                forceNotNull?.takeIf { it.columns.isNotEmpty() }
                    ?.let { append(", FORCE_NOT_NULL '").append(it).append('\'') }
                forceNull?.takeIf { it.columns.isNotEmpty() }
                    ?.let { append(", FORCE_NULL '").append(it).append('\'') }
                append(')')
            }
    }

    /**
     * [CopyStatement] implementation for copying from STDIN as text delimited into the table
     * specified
     */
    data class TableFromText(
        override val schemaName: String,
        override val tableName: String,
        override val columnNames: List<String> = emptyList(),
        override val delimiter: Char = '\t',
        override val nullString: String = "\\N",
        override val default: String? = null,
        override val header: CopyHeader? = null,
    ) : From, CopyTable, CopyText {
        override val format: CopyFormat = CopyFormat.Text

        override fun toQuery(): String =
            buildString {
                append("COPY ")
                    .append(schemaName.quoteIdentifier())
                    .append('.')
                    .append(tableName.quoteIdentifier())
                if (columnNames.isNotEmpty()) {
                    columnNames.joinTo(buffer = this, separator = ",", prefix = "(", postfix = ")")
                }
                append(" FROM STDIN")
                appendTextOptions(this)
            }
    }

    /**
     * [CopyStatement] implementation for copying from STDIN as binary data into the table
     * specified
     */
    data class TableFromBinary(
        override val schemaName: String,
        override val tableName: String,
        override val columnNames: List<String> = emptyList(),
    ) : From, CopyTable {
        override val format: CopyFormat = CopyFormat.Binary

        override fun toQuery(): String =
            buildString {
                append("COPY ")
                    .append(schemaName.quoteIdentifier())
                    .append('.')
                    .append(tableName.quoteIdentifier())
                if (columnNames.isNotEmpty()) {
                    columnNames.joinTo(buffer = this, separator = ",", prefix = "(", postfix = ")")
                }
                append(" FROM STDIN (FORMAT binary)")
            }
    }
}

private fun CopyStatement.CopyCsv.appendCsvOptions(builder: StringBuilder) =
    builder.apply {
        append(" WITH (FORMAT csv")
        if (delimiter == '\'') {
            append(", DELIMITER ''''")
        } else {
            append(", DELIMITER '").append(delimiter).append('\'')
        }
        append(", NULL '").append(nullString.replace("'", "''")).append('\'')
        default?.let {
            append(", DEFAULT '").append(it.replace("'", "''")).append('\'')
        }
        header?.let {
            append(", HEADER ").append(it)
        }
        if (quote == '\'') {
            append(", QUOTE ''''")
        } else {
            append(", QUOTE '").append(quote).append('\'')
        }
        if (escape == '\'') {
            append(", ESCAPE ''''")
        } else {
            append(", ESCAPE '").append(escape).append('\'')
        }
    }

private fun CopyStatement.CopyText.appendTextOptions(builder: StringBuilder) =
    builder.apply {
        append(" WITH (FORMAT text")
        if (delimiter == '\'') {
            append(", DELIMITER ''''")
        } else {
            append(", DELIMITER '").append(delimiter).append('\'')
        }
        append(", NULL '").append(nullString.replace("'", "''")).append('\'')
        default?.let {
            append(", DEFAULT '").append(it.replace("'", "''")).append('\'')
        }
        header?.let {
            append(", HEADER ").append(it)
        }
        append(')')
    }

/**
 * Magic header value required at the start a binary COPY operation
 *
 * [docs](https://www.postgresql.org/docs/current/sql-copy.html)
 */
val pgBinaryCopyHeader =
    byteArrayOf(
        'P'.code.toByte(),
        'G'.code.toByte(),
        'C'.code.toByte(),
        'O'.code.toByte(),
        'P'.code.toByte(),
        'Y'.code.toByte(),
        0x0A,
        -1,
        0x0D,
        0x0A,
        0x00,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    )

/**
 * Magic trailer value required before the end of a binary COPY operation
 *
 * [docs](https://www.postgresql.org/docs/current/sql-copy.html)
 */
val pgBinaryCopyTrailer = byteArrayOf(-1, -1)
