package com.github.clasicrando.common.atomic

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update

/**
 * Mutable map with an [AtomicRef] wrapping an immutable [Map]. The underlining [Map] is exposed as
 * a [MutableMap] where update operations are handled with an atomic [AtomicRef.update] method call.
 *
 * NOTE
 * - for [entries], [keys] and [values], a new [MutableMap] is used to return the same properties on
 * that [Map] to avoid iterating or accessing those views while concurrent updates happen. This
 * means the contents you are iterating over might not match the exact contents of the [Map] at the
 * time of iteration. If you need that consistency, you should use a suspending mutex backed [Map].
 */
internal class AtomicMutableMap<K, V>(initial: Map<K, V> = emptyMap()) : MutableMap<K, V> {
    private val inner: AtomicRef<Map<K, V>> = atomic(initial)

    constructor(vararg items: Pair<K, V>): this(items.toMap())
    constructor(iterable: Iterable<Pair<K, V>>): this(iterable.toMap())

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
