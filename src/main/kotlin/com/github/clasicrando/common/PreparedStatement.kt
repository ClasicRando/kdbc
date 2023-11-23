package com.github.clasicrando.common

interface PreparedStatement {
    val query: String
    val statementId: String
    val paramCount: Int
    var prepared: Boolean
}
