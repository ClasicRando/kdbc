package com.github.clasicrando.common.exceptions

import kotlinx.uuid.UUID

class ConnectionRunningQuery(connectionId: UUID)
    : Throwable("$connectionId - Connection is still executing a query")
