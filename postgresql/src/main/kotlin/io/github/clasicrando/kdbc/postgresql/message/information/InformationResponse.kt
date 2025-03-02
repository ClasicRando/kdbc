package io.github.clasicrando.kdbc.postgresql.message.information

/**
 * Data contained within [io.github.clasicrando.kdbc.postgresql.message.PgMessage.ErrorResponse] and
 * [io.github.clasicrando.kdbc.postgresql.message.PgMessage.NoticeResponse] messages. Data is
 * derived from a [Map] of [Byte] field identifiers linked values that match the expected values as
 * described [here](https://www.postgresql.org/docs/current/protocol-error-fields.html).
 */
@Suppress("MemberVisibilityCanBePrivate")
public class InformationResponse
internal constructor(
    /** Severity of the message */
    public val severity: Severity,
    /** SQLSTATE code of the message */
    public val code: SqlState,
    /** Human-readable version of the message */
    public val message: String,
    /** Optional extra details along with the message */
    public val detail: String?,
    /** Optional suggestion about the problem */
    public val hint: String?,
    /** Error cursor position within the original query string. Index is character not bytes */
    public val position: Int?,
    /**
     * [Pair] where the first value is the error cursor position within the internal command and the
     * second value is the internal command's query (e.g. the SQL query within a PL/pgsql function).
     */
    public val internalQueryData: Pair<Int, String>?,
    /**
     * Call stack traceback of the active procedural language function or internal-generated query
     */
    public val where: String?,
    /**
     * If the message is associated with a specific database object, this is the name of the schema
     * containing the object
     */
    public val schemaName: String?,
    /**
     * If the message is associated with a specific database table, this is the name of the table
     */
    public val tableName: String?,
    /** If the message is associated with a specific table column, this is the name of the column */
    public val columnName: String?,
    /** If the message is associated with a specific data type, this is the name of the data type */
    public val dataTypeName: String?,
    /**
     * If the message is associated with a specific constraint, this is the name of the constraint
     */
    public val constraintName: String?,
    /** The file name of the source code where the error was reported */
    public val file: String?,
    /** The line number of the source code where the error was reported */
    public val line: Int?,
    /** THe name of the source code routine reporting the error */
    public val routine: String?,
) {
    /**
     * Construct a new [InformationResponse] using the [fields] [Map] to populate each field.
     *
     * @throws InvalidInformationResponse
     */
    internal constructor(
        fields: Map<Byte, String>
    ) : this(
        severity =
            fields[SEVERITY]?.let { Severity.valueOf(it) }
                ?: fields[SEVERITY2]?.let { Severity.valueOf(it) }
                ?: throw InvalidInformationResponse(SEVERITY),
        code =
            fields[CODE]?.let { SqlState.fromCode(it) } ?: throw InvalidInformationResponse(CODE),
        message = fields[MESSAGE] ?: throw InvalidInformationResponse(MESSAGE),
        detail = fields[DETAIL],
        hint = fields[HINT],
        position = fields[POSITION]?.toIntOrNull(),
        internalQueryData =
            fields[INTERNAL_POSITION]?.toIntOrNull()?.let {
                val internalQuery =
                    fields[INTERNAL_QUERY] ?: throw InvalidInformationResponse(INTERNAL_QUERY)
                it to internalQuery
            },
        where = fields[WHERE],
        schemaName = fields[SCHEMA],
        tableName = fields[TABLE],
        columnName = fields[COLUMN],
        dataTypeName = fields[DATE_TYPE],
        constraintName = fields[CONSTRAINT_NAME],
        file = fields[FILE],
        line = fields[LINE]?.toIntOrNull(),
        routine = fields[ROUTINE],
    )

    override fun toString(): String {
        val internalQueryString =
            internalQueryData?.let { "Position=${it.first}, Query=${it.second}" } ?: ""
        return """
            Severity: $severity
            SQL State: ${code.errorCode} -> ${code.conditionName}
            Message: $message
            Detail: $detail
            Hint: $hint
            Position: $position
            Internal Query Data: $internalQueryString
            Where: $where
            Schema: $schemaName
            Table: $tableName
            Column: $columnName
            Data Type: $dataTypeName
            Constraint: $constraintName
            File: $file
            Line: $line
            Routine: $routine
            """
            .trimIndent()
    }

    private companion object {
        private const val SEVERITY = 'S'.code.toByte()
        private const val SEVERITY2 = 'V'.code.toByte()
        private const val CODE = 'C'.code.toByte()
        private const val MESSAGE = 'M'.code.toByte()
        private const val DETAIL = 'D'.code.toByte()
        private const val HINT = 'H'.code.toByte()
        private const val POSITION = 'P'.code.toByte()
        private const val INTERNAL_POSITION = 'p'.code.toByte()
        private const val INTERNAL_QUERY = 'q'.code.toByte()
        private const val WHERE = 'W'.code.toByte()
        private const val SCHEMA = 's'.code.toByte()
        private const val TABLE = 't'.code.toByte()
        private const val COLUMN = 'c'.code.toByte()
        private const val DATE_TYPE = 'd'.code.toByte()
        private const val CONSTRAINT_NAME = 'n'.code.toByte()
        private const val FILE = 'F'.code.toByte()
        private const val LINE = 'L'.code.toByte()
        private const val ROUTINE = 'R'.code.toByte()
    }
}
