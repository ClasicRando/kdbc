package com.github.clasicrando.common.atomic

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update

internal class AtomicMutableMap<K, V>(
    vararg initial: Pair<K, V>
) : MutableMap<K, V> {
    private val inner = atomic(mapOf(*initial))

    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() = inner.value.toMutableMap().entries
    override val keys: MutableSet<K>
        get() = inner.value.toMutableMap().keys
    override val size: Int
        get() = inner.value.size
    override val values: MutableCollection<V>
        get() = inner.value.toMutableMap().values

    override fun clear() {
        inner.update { mapOf() }
    }

    override fun isEmpty(): Boolean = inner.value.isEmpty()

    override fun remove(key: K): V? {
        var result: V? = null
        inner.update { current ->
            result = current[key] ?: return@update current
            current.filter { it.key != key }
        }
        return result
    }

    override fun putAll(from: Map<out K, V>) {
        inner.update { current ->
            current.plus(from)
        }
    }

    override fun put(key: K, value: V): V? {
        var result: V? = null
        inner.update { current ->
            result = current[key]
            current.plus(key to value)
        }
        return result
    }

    override fun get(key: K): V? = inner.value[key]

    override fun containsValue(value: V): Boolean = inner.value.containsValue(value)

    override fun containsKey(key: K): Boolean = inner.value.containsKey(key)
}
