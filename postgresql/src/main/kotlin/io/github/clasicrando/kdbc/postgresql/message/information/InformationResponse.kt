package io.github.clasicrando.kdbc.postgresql.message.information

import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * Data contained within [PgMessage.ErrorResponse] and [PgMessage.NoticeResponse] messages. Data
 * is derived from a [Map] of [Byte] field identifiers linked values that match the expected values
 * as described [here](https://www.postgresql.org/docs/current/protocol-error-fields.html).
 */
@Suppress("MemberVisibilityCanBePrivate")
class InformationResponse internal constructor(
    /** Severity of the message */
    val severity: Severity,
    /** SQLSTATE code of the message */
    val code: SqlState,
    /** Human-readable version of the message */
    val message: String,
    /** Optional extra details along with the message */
    val detail: String?,
    /** Optional suggestion about the problem */
    val hint: String?,
    /** Error cursor position within the original query string. Index is character not bytes */
    val position: Int?,
    /**
     * [Pair] where the first value is the error cursor position within the internal command and
     * the second value is the internal command's query (e.g. the SQL query within a PL/pgsql
     * function).
     */
    val internalQueryData: Pair<Int, String>?,
    /**
     * Call stack traceback of the active procedural language function or internal-generated
     * query
     */
    val where: String?,
    /**
     * If the message is associated with a specific database object, this is the name of the schema
     * containing the object
     */
    val schemaName: String?,
    /**
     * If the message is associated with a specific database table, this is the name of the table
     */
    val tableName: String?,
    /**
     * If the message is associated with a specific table column, this is the name of the column
     */
    val columnName: String?,
    /**
     * If the message is associated with a specific data type, this is the name of the data type
     */
    val dataTypeName: String?,
    /**
     * If the message is associated with a specific constraint, this is the name of the constraint
     */
    val constraintName: String?,
    /** The file name of the source code where the error was reported */
    val file: String?,
    /** The line number of the source code where the error was reported */
    val line: Int?,
    /** THe name of the source code routine reporting the error */
    val routine: String?,
) {
    /**
     * Construct a new [InformationResponse] using the [fields] [Map] to populate each field.
     *
     * @throws InvalidInformationResponse
     */
    internal constructor(fields: Map<Byte, String>) :
        this(
            severity = fields[SEVERITY]?.let { Severity.valueOf(it) }
                ?: fields[SEVERITY2]?.let { Severity.valueOf(it) }
                ?: throw InvalidInformationResponse(SEVERITY),
            code = fields[CODE]?.let { SqlState.fromCode(it) }
                ?: throw InvalidInformationResponse(CODE),
            message = fields[MESSAGE] ?: throw InvalidInformationResponse(MESSAGE),
            detail = fields[DETAIL],
            hint = fields[HINT],
            position = fields[POSITION]?.toIntOrNull(),
            internalQueryData = fields[INTERNAL_POSITION]
                ?.toIntOrNull()
                ?.let {
                    val internalQuery = fields[INTERNAL_QUERY]
                        ?: throw InvalidInformationResponse(INTERNAL_QUERY)
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
        val internalQueryString = internalQueryData?.let {
            "Position=${it.first}, Query=${it.second}"
        } ?: ""
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
        """.trimIndent()
    }

    companion object {
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
