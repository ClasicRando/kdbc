package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.core.statement.PreparedStatement
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import kotlinx.datetime.LocalDateTime

/** Postgresql implementation of a [PreparedStatement] */
internal class PgPreparedStatement(
    override val query: String,
    override val statementId: UInt,
) : PreparedStatement {
    override val paramCount = PARAM_COUNT_REGEX.findAll(query).count()
    override var prepared = false
    var parameterTypeOids: List<Int> = emptyList()
    var resultMetadata: List<PgColumnDescription> = emptyList()
    /** Name value used to construct the prepared statement on the server side */
    val statementName = statementId.toString()
    override var lastExecuted: LocalDateTime? = null

    override fun toString(): String {
        return "PgPreparedStatement(query=\"$query\",statementId=$statementId)"
    }

    companion object {
        /** Regex to find sections of text that match the postgresql parameter syntax */
        private val PARAM_COUNT_REGEX = Regex("\\$\\d+")
    }
}
