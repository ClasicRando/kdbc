package com.github.clasicrando.common.pool

import com.github.clasicrando.common.Connection

internal interface PoolConnection : Connection {
    var pool: ConnectionPool?
}
