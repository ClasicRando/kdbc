package com.github.clasicrando.common.exceptions

import kotlinx.uuid.UUID

/**
 * Exception thrown when connection is not running query but operation assumes a query is currently
 * being run.
 */
class ConnectionNotRunningQuery(connectionId: UUID)
    : Throwable("$connectionId - Connection is not running a query")
