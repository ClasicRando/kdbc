package com.clasicrando.kdbc

import javax.sql.DataSource
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

val CoroutineContext.dataSource: DataSource
    get() = get(CoroutineDataSource)?.dataSource ?: error("Could not find a DataSource for the current context")

/**
 * [CoroutineContext] element holding a [DataSource]. Used to pass along the same dataSource to a coroutine even when
 * switching contexts, resuming execution or switching [Dispatchers][kotlinx.coroutines.Dispatchers]
 */
class CoroutineDataSource(val dataSource: DataSource) : AbstractCoroutineContextElement(CoroutineDataSource) {

    companion object Key: CoroutineContext.Key<CoroutineDataSource>

    override fun toString() = "CoroutineDataSource($dataSource)"

}
