package com.github.clasicrando.common.exceptions

import kotlinx.uuid.UUID

class ConnectionNotRunningQuery(connectionId: UUID)
    : Throwable("$connectionId - Connection is not running a query")
