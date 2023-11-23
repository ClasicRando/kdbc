package com.github.clasicrando.common.atomic

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.update

class AtomicMutableSet<E>(vararg init: E) : MutableSet<E> {
    private val inner = atomic(setOf(*init))

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
            result = initSize < new.size
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
}
