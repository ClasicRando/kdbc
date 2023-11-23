package com.github.clasicrando.postgresql

import com.github.clasicrando.common.PreparedStatement
import com.github.clasicrando.postgresql.row.PgRowFieldDescription
import java.util.UUID

class PgPreparedStatement(
    override val query: String,
    override val statementId: String = UUID.randomUUID().toString(),
) : PreparedStatement {
    override val paramCount = PARAM_COUNT_REGEX.findAll(query).count()
    override var prepared = false
    var metadata: List<PgRowFieldDescription> = emptyList()

    override fun toString(): String {
        return "PgPreparedStatement(query=\"$query\",statementId=$statementId)"
    }

    companion object {
        private val PARAM_COUNT_REGEX = Regex("\\$\\d+")
    }
}
