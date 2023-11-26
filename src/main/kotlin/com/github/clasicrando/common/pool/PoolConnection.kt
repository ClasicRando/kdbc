package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection

internal interface PoolConnection : Connection {
    var pool: ConnectionPool?
}
