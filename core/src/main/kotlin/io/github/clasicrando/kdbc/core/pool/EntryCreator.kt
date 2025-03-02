package io.github.clasicrando.kdbc.core.pool

internal interface EntryCreator {
    suspend fun requestCreate(waiting: Int)
}
