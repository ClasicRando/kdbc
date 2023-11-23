package com.github.clasicrando.common.exceptions

class ConnectionNotRunningQuery(connectionId: String)
    : Throwable("$connectionId - Connection is not running a query")
