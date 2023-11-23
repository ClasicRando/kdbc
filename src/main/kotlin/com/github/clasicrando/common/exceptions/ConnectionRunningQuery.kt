package com.github.clasicrando.common.exceptions

class ConnectionRunningQuery(connectionId: String)
    : Throwable("$connectionId - Connection is still executing a query")
