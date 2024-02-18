package com.github.clasicrando.postgresql.statement

import com.github.clasicrando.common.statement.PreparedStatement
import com.github.clasicrando.postgresql.row.PgColumnDescription

internal class PgPreparedStatement(
    override val query: String,
    override val statementId: UInt,
) : PreparedStatement {
    override val paramCount = PARAM_COUNT_REGEX.findAll(query).count()
    override var prepared = false
    var parameterTypeOids: List<Int> = emptyList()
    var resultMetadata: List<PgColumnDescription> = emptyList()
    val statementName = statementId.toString()

    override fun toString(): String {
        return "PgPreparedStatement(query=\"$query\",statementId=$statementId)"
    }

    companion object {
        private val PARAM_COUNT_REGEX = Regex("\\$\\d+")
    }
}
