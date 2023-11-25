package com.github.clasicrando.common

import kotlinx.uuid.UUID

interface PreparedStatement {
    val query: String
    val statementId: UUID
    val paramCount: Int
    var prepared: Boolean
}
