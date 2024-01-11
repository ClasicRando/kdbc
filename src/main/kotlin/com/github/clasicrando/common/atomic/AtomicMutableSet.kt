package com.github.clasicrando.common.atomic

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update

/**
 * Mutable set with an [AtomicRef] wrapping an immutable [Set]. The underlining [Set] is exposed as
 * a [MutableSet] where update operations are handled with an atomic [AtomicRef.update] method call.
 *
 * NOTE
 * - for [iterator], a new [MutableSet] is used to return the same unique entries in the backing
 * [Set] to avoid iterating or accessing that views while concurrent updates happen. This means the
 * contents you are iterating over might not match the exact contents of the [Set] at the time of
 * iteration. If you need that consistency, you should use a suspending mutex backed [Set].
 */
internal class AtomicMutableSet<E>(initial: Set<E> = emptySet()) : MutableSet<E> {
    private val inner: AtomicRef<Set<E>> = atomic(initial)

    constructor(vararg init: E): this(init.toSet())
    constructor(iterable: Iterable<E>): this(iterable.toSet())

    override fun add(element: E): Boolean {
        var result = false
        inner.update { current ->
            if (current.contains(element)) {
                return@update current
            }
            result = true
            current.plus(element)
        }
        return result
    }

    override fun addAll(elements: Collection<E>): Boolean {
        var result = false
        inner.update { current ->
            val initSize = current.size
            val new = current.plus(elements)
            result = new.size > initSize
            new
        }
        return result
    }

    override val size: Int get() = inner.value.size

    override fun clear() {
        inner.update { setOf() }
    }

    override fun isEmpty(): Boolean = inner.value.isEmpty()

    override fun containsAll(elements: Collection<E>): Boolean = inner.value.containsAll(elements)

    override fun contains(element: E): Boolean = inner.value.contains(element)

    override fun iterator(): MutableIterator<E> = inner.value.toMutableSet().iterator()

    override fun retainAll(elements: Collection<E>): Boolean {
        var removed = false
        inner.update { current ->
            removed = current.any { !elements.contains(it) }
            if (!removed) {
                return@update current
            }
            elements.toSet()
        }
        return removed
    }

    override fun removeAll(elements: Collection<E>): Boolean {
        var contains = false
        inner.update { current ->
            buildSet {
                addAll(current)
                contains = removeAll(elements.toSet())
            }
        }
        return contains
    }

    override fun remove(element: E): Boolean {
        var contains = false
        inner.update { current ->
            buildSet {
                addAll(current)
                contains = remove(element)
            }
        }
        return contains
    }

    override fun toString(): String {
        return "AtomicMutableSet(${inner.value.toList().joinToString(prefix = "[", postfix = "]")})"
    }
}
