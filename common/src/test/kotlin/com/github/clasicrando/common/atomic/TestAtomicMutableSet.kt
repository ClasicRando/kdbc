package com.github.clasicrando.common.atomic

import com.github.clasicrando.randomString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TestAtomicMutableSet {
    private fun randomStringSet(size: Int): Set<String> {
        return (1..size).map { randomString() }.toSet()
    }

    @Test
    fun `add should add item and return false when item already exists`() {
        val setSize = 100
        val set = randomStringSet(setSize)
        val existingItem = set.first()
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.add(existingItem)

        assertFalse(result)
        assertEquals(setSize, atomicSet.size)
        assertTrue(atomicSet.contains(existingItem))
    }

    @Test
    fun `add should add item and return true when does not already exist`() {
        val setSize = 100
        val set = randomStringSet(setSize)
        val nonExistentItem = randomString(1)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.add(nonExistentItem)

        assertTrue(result)
        assertEquals(setSize + 1, atomicSet.size)
        assertTrue(atomicSet.contains(nonExistentItem))
    }

    @Test
    fun `addAll should add all items and return true when at least 1 item is added`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val nonExistentItem = randomString(1)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.addAll(set.take(1).plus(nonExistentItem))

        assertTrue(result)
        assertEquals(setSize + 1, atomicSet.size)
        assertTrue(atomicSet.contains(nonExistentItem))
    }

    @Test
    fun `addAll should add nothing and return false when all items exist in set`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.addAll(set.take(5))

        assertFalse(result)
        assertEquals(setSize, atomicSet.size)
    }

    @Test
    fun `clear should remove all items and leave empty set`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val atomicSet = AtomicMutableSet(set)

        atomicSet.clear()

        assertTrue(atomicSet.isEmpty())
    }

    @Test
    fun `retainAll should keep all items and return false when other collection contains all items`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.retainAll(set)

        assertFalse(result)
        assertEquals(setSize, atomicSet.size)
    }

    @Test
    fun `retainAll should only contain elements from other collection and return true when other collection contains less`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.retainAll(set.take(setSize - 2))

        assertTrue(result)
        assertEquals(setSize - 2, atomicSet.size)
    }

    @Test
    fun `removeAll should remove items and return true when at least 1 item from other present`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val otherItem = randomString(2)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.removeAll(set.take(1).plus(otherItem))

        assertTrue(result)
        assertEquals(setSize - 1, atomicSet.size)
    }

    @Test
    fun `removeAll should remove nothing and return false when no item in other present`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val otherItems = randomStringSet(2)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.removeAll(otherItems)

        assertFalse(result)
        assertEquals(setSize, atomicSet.size)
    }

    @Test
    fun `remove should remove item and return true when item present`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.remove(set.first())

        assertTrue(result)
        assertEquals(setSize - 1, atomicSet.size)
    }

    @Test
    fun `remove should remove nothing and return false when item not present`() {
        val setSize = 10
        val set = randomStringSet(setSize)
        val otherItem = randomString(2)
        val atomicSet = AtomicMutableSet(set)

        val result = atomicSet.remove(otherItem)

        assertFalse(result)
        assertEquals(setSize, atomicSet.size)
    }
}
