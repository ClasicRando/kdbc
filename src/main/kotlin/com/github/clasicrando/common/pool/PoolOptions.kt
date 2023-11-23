package com.github.clasicrando.common.pool

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlin.time.DurationUnit
import kotlin.time.toDuration

data class PoolOptions(
    val maxConnections: Int,
    val minConnections: Int = 0,
    val acquireTimeout: Long = Long.MAX_VALUE,
    val idleTime: Long = 1.toDuration(DurationUnit.MINUTES).inWholeMilliseconds,
    val coroutineDispatcher: CoroutineDispatcher = Dispatchers.IO,
    val parentScope: CoroutineScope? = null,
)
