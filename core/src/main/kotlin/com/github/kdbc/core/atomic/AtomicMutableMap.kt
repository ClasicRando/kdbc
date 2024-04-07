package com.github.kdbc.core.atomic

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update

/**
 * [MutableMap] implementation using an [AtomicRef] wrapping the underlining [Map] that is updated
 * using [AtomicRef.update] to make operations atomic.
 *
 * NOTE
 * - for [entries], [keys] and [values], a new [MutableMap] is used to return the same properties
 * on that [Map] to avoid iterating or accessing those views while concurrent updates happen. This
 * means the contents you are iterating over might not match the exact contents of the [Map] at the
 * time of iteration. If you need that consistency, you should use a suspending mutex backed [Map].
 */
class AtomicMutableMap<K, V>(initial: Map<K, V> = emptyMap()) : MutableMap<K, V> {
    private val inner: AtomicRef<Map<K, V>> = atomic(initial)

    /**
     * Returns a copy of the underling [Map]'s [Map.entries]. Does not keep consistent with future
     * updates to this [MutableMap].
     */
    override val entries: MutableSet<MutableMap.MutableEntry<K, V>>
        get() = inner.value.toMutableMap().entries
    /**
     * Returns a copy of the underling [Map]'s [Map.keys]. Does not keep consistent with future
     * updates to this [MutableMap].
     */
    override val keys: MutableSet<K>
        get() = inner.value.toMutableMap().keys
    override val size: Int
        get() = inner.value.size
    /**
     * Returns a copy of the underling [Map]'s [Map.values]. Does not keep consistent with future
     * updates to this [MutableMap].
     */
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
