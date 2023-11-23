package com.github.clasicrando.common.pool

import com.github.clasicrando.common.Connection
import kotlinx.coroutines.CoroutineScope

interface ConnectionFactory {
    suspend fun create(scope: CoroutineScope): Connection
    suspend fun validate(item: Connection): Boolean
}
