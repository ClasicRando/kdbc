package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.quoteIdentifier

/**
 * Constructor of valid COPY statements for the Postgresql extended query feature. See the
 * postgresql [docs](https://www.postgresql.org/docs/current/sql-copy.html) for more information.
 * Most of the parameter descriptions were taken from the Postgresql docs if they represent the
 * exact option or query component.
 *
 * @param tableName Target table of the COPY operation, optionally schema qualified. Must already
 * exist.
 * @param columnNames Optional [List] of column names that will be copied. If no column list is
 * specified then all columns of the table except for the generated columns will be copied.
 * @param format Specifies the data format to be read or written. Default is [CopyFormat.Text]
 * @param delimiter Specifies the character that separates columns within each row of the file. The
 * default character is tab when in [CopyFormat.Text] and comma when [CopyFormat.CSV]. If the
 * format is [CopyFormat.Binary] this supplied value is ignored.
 * @param nullString Specifies the string that represents a null value. The default is "\N"
 * (backslash-N) in [CopyFormat.Text] format, and an unquoted empty string in [CopyFormat.CSV]. You
 * might prefer an empty string even in text format for cases where you don't want to distinguish
 * nulls from empty strings. This option is ignored when using [CopyFormat.Binary].
 * @param default Specifies the string that represents a default value. Each time the string is
 * found in the input file, the default value of the corresponding column will be used. This option
 * is ignored unless the type is [CopyType.From], and the format is not [CopyFormat.Binary].
 * @param header Specifies that the file contains a header line with the names of each column in
 * the file. On output, the first line contains the column names from the table. On input, the
 * first line is discarded when this option is set to [CopyHeader.TRUE]. If this option is set to
 * [CopyHeader.MATCH], the number and names of the columns in the header line must match the actual
 * column names of the table, in order; otherwise an error is raised. This option is ignored when
 * [CopyFormat.Binary]. The [CopyHeader.MATCH] option is only valid for [CopyType.From].
 * @param quote Specifies the quoting character to be used when a data value is quoted. The default
 * is double-quote. This must be a single one-byte character. This option is ignored unless the
 * format is [CopyFormat.CSV].
 * @param escape Specifies the character that should appear before a data character that matches
 * the [quote] value. The default is the same as the [quote] value (so that the quoting character
 * is doubled if it appears in the data). This option is allowed only when using [CopyFormat.CSV].
 * @param forceQuote Forces quoting to be used for all non-NULL values in each specified column.
 * NULL output is never quoted. If [ForceAgainstColumns.All] is specified, non-NULL values will be
 * quoted in all columns. This option is allowed only when [CopyType.To], and only when using
 * [CopyFormat.CSV], otherwise the value is ignored.
 * @param forceNotNull Do not match the specified columns' values against the null string. In the
 * default case where the null string is empty, this means that empty values will be read as
 * zero-length strings rather than nulls, even when they are not quoted. This option is allowed
 * only when [CopyType.From], and only when using [CopyFormat.CSV], otherwise the value is ignored.
 * @param forceNull Match the specified columns' values against the null string, even if it has
 * been quoted, and if a match is found set the value to NULL. In the default case where the null
 * string is empty, this converts a quoted empty string into NULL. This option is allowed only when
 * [CopyType.From], and only when using [CopyFormat.CSV], otherwise the value is ignored.
 */
class CopyStatement(
    private val tableName: String,
    private val schemaName: String? = null,
    private val columnNames: List<String> = emptyList(),
    private val format: CopyFormat = CopyFormat.Text,
    delimiter: Char? = null,
    nullString: String? = null,
    default: String? = null,
    header: CopyHeader? = null,
    quote: Char? = null,
    escape: Char? = null,
    forceQuote: ForceAgainstColumns? = null,
    forceNotNull: ForceAgainstColumns.Select? = null,
    forceNull: ForceAgainstColumns.Select? = null,
) {
    private val delimiter = when (format) {
        CopyFormat.Text -> delimiter ?: '\t'
        CopyFormat.CSV -> delimiter ?: ','
        CopyFormat.Binary -> null
    }
    private val nullString = when (format) {
        CopyFormat.Text -> nullString ?: "\\N"
        CopyFormat.CSV -> nullString ?: ""
        CopyFormat.Binary -> null
    }
    private val default = when (format) {
        CopyFormat.Binary -> null
        else -> default
    }
    private val header = when (format) {
        CopyFormat.Binary -> null
        else -> header
    }
    private val quote = when (format) {
        CopyFormat.CSV -> quote ?: '"'
        else -> null
    }
    private val escape = when (format) {
        CopyFormat.CSV -> escape ?: quote
        else -> null
    }
    private val forceQuote = when (format) {
        CopyFormat.CSV -> forceQuote
        else -> null
    }
    private val forceNotNull = when (format) {
        CopyFormat.CSV -> forceNotNull
        else -> null
    }
    private val forceNull = when (format) {
        CopyFormat.CSV -> forceNull
        else -> null
    }

    override fun toString(): String {
        val columns = columnNames.joinToString(separator = ",", prefix = "[", postfix = "]")
        return buildString {
            append("CopyStatement(tableName=$tableName,columnNames=$columns,format=$format,")
            append("delimiter=$delimiter,nullString=$nullString,default=$default,header=$header,")
            append("quote=$quote,escape=$escape,forceQuote=$forceQuote,forceNotNull=$forceNotNull")
            append("forceNull=$forceNull)")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (other !is CopyStatement) return false
        return this.tableName == other.tableName
                && this.columnNames == other.columnNames
                && this.format == other.format
                && this.delimiter == other.delimiter
                && this.nullString == other.nullString
                && this.default == other.default
                && this.header == other.header
                && this.quote == other.quote
                && this.escape == other.escape
                && this.forceQuote == other.forceQuote
                && this.forceNotNull == other.forceNotNull
                && this.forceNull == other.forceNull
    }

    override fun hashCode(): Int {
        var result = tableName.hashCode()
        result = 31 * result + columnNames.hashCode()
        result = 31 * result + format.hashCode()
        result = 31 * result + (delimiter?.hashCode() ?: 0)
        result = 31 * result + (nullString?.hashCode() ?: 0)
        result = 31 * result + (default?.hashCode() ?: 0)
        result = 31 * result + (header?.hashCode() ?: 0)
        result = 31 * result + (quote?.hashCode() ?: 0)
        result = 31 * result + (escape?.hashCode() ?: 0)
        result = 31 * result + (forceQuote?.hashCode() ?: 0)
        result = 31 * result + (forceNotNull?.hashCode() ?: 0)
        result = 31 * result + (forceNull?.hashCode() ?: 0)
        return result
    }

    /**
     * Using the supplied values and options, construct a COPY query that can be sent to the
     * Postgresql database to initiate the extended query functionality.
     */
    fun toStatement(copyType: CopyType): String = buildString {
        append("COPY ")
        schemaName?.let {
            append(it.quoteIdentifier())
            append('.')
        }
        append(tableName.quoteIdentifier())
        columnNames.takeIf { it.isNotEmpty() }?.let {
            append(it.joinToString(separator = ",", prefix = "(", postfix = ")"))
        }
        val direction = when (copyType) {
            CopyType.From -> " FROM STDIN "
            CopyType.To -> " TO STDOUT "
        }
        append(direction)
        append("WITH (FORMAT ")
        append(format.toString())
        delimiter?.let {
            if (it == '\'') {
                append(", DELIMITER ''''")
                return@let
            }
            append(", DELIMITER '$it'")
        }
        nullString?.let {
            append(", NULL '${it.replace("'", "''")}'")
        }
        default?.let {
            append(", DEFAULT '${it.replace("'", "''")}'")
        }
        header?.let {
            append(", HEADER $it")
        }
        quote?.let {
            if (it == '\'') {
                append(", QUOTE ''''")
                return@let
            }
            append(", QUOTE '$it'")
        }
        escape?.let {
            if (it == '\'') {
                append(", ESCAPE ''''")
                return@let
            }
            append(", ESCAPE '$it'")
        }
        if (copyType == CopyType.To && format == CopyFormat.CSV && forceQuote != null) {
            append(", FORCE_QUOTE '$forceQuote'")
        }
        if (copyType == CopyType.From && format == CopyFormat.CSV) {
            forceNotNull?.takeIf { it.columns.isNotEmpty() }
                ?.let { append(", FORCE_NOT_NULL '$it'") }
            forceNull?.takeIf { it.columns.isNotEmpty() }
                ?.let { append(", FORCE_NULL '$it'") }
        }
        append(")")
    }
}
