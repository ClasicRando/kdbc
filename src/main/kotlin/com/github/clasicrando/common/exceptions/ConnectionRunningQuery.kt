package com.github.clasicrando.common.exceptions

import kotlinx.uuid.UUID

/**
 * Exception thrown when connection is running query but another coroutine is trying to contact the
 * server
 */
class ConnectionRunningQuery(connectionId: UUID)
    : Throwable("$connectionId - Connection is still executing a query")
