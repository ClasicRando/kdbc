package io.github.clasicrando.kdbc.mysql.statement

import io.github.clasicrando.kdbc.core.statement.PreparedStatement
import io.github.clasicrando.kdbc.mysql.result.MySqlColumn

/** MySQL implementation of a [PreparedStatement] */
internal class MySqlPreparedStatement(
    override val query: String,
    override val statementId: Int,
    override val paramCount: Int,
    override val resultMetadata: List<MySqlColumn>,
) : PreparedStatement
