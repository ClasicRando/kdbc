package io.github.clasicrando.kdbc.mysql.statement

import io.github.clasicrando.kdbc.core.statement.PreparedStatement
import io.github.clasicrando.kdbc.mysql.result.MySqlColumn
import kotlinx.datetime.Instant

internal class MySqlPreparedStatement(
    override val query: String,
    override val statementId: Int,
    override val paramCount: Int,
    val columns: List<MySqlColumn>,
    val columnNames: Map<String, Int>,
) : PreparedStatement {
    override var prepared = true
    override var lastExecuted: Instant? = null
}
