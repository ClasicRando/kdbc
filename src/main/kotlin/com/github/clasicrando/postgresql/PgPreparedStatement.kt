package com.github.clasicrando.postgresql

import com.github.clasicrando.common.statement.PreparedStatement
import com.github.clasicrando.postgresql.row.PgRowFieldDescription
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID

class PgPreparedStatement(
    override val query: String,
    override val statementId: UUID = UUID.generateUUID(),
) : PreparedStatement {
    override val paramCount = PARAM_COUNT_REGEX.findAll(query).count()
    override var prepared = false
    var metadata: List<PgRowFieldDescription> = emptyList()
    val statementName = statementId.toString()

    override fun toString(): String {
        return "PgPreparedStatement(query=\"$query\",statementId=$statementId)"
    }

    companion object {
        private val PARAM_COUNT_REGEX = Regex("\\$\\d+")
    }
}
