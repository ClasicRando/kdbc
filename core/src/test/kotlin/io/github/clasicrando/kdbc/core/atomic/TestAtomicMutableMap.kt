package io.github.clasicrando.kdbc.core.atomic

import io.github.clasicrando.kdbc.core.randomString
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TestAtomicMutableMap {
    private fun randomIntKeyMap(
        size: Int,
        offset: Int = 0,
    ): Map<Int, String> {
        return (1..size).associate { (it + offset) to randomString() }
    }

    @Test
    fun `clear should update to empty map`() {
        val initialMap = randomIntKeyMap(100_00)
        val origMap = AtomicMutableMap(initialMap)

        origMap.clear()

        assertTrue(origMap.isEmpty())
    }

    @Test
    fun `remove should leave map without specified key and return value when key present`() {
        val mapSize = 100
        val initialMap = randomIntKeyMap(mapSize)
        val origMap = AtomicMutableMap(initialMap)
        val removalKey = Random.nextInt(1, initialMap.size)

        val removedItem = origMap.remove(removalKey)

        assertEquals(initialMap[removalKey], removedItem)
        assertFalse(origMap.containsKey(removalKey))
        assertEquals(mapSize - 1, origMap.size)
    }

    @Test
    fun `remove should do nothing when key not present`() {
        val mapSize = 100
        val initialMap = randomIntKeyMap(mapSize)
        val origMap = AtomicMutableMap(initialMap)
        val removalKey = mapSize + 1

        val removedItem = origMap.remove(removalKey)

        assertNull(removedItem)
        assertEquals(mapSize, origMap.size)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `putAll should add all items`(keysIntersect: Boolean) {
        val mapSize = 100
        val initialMap = randomIntKeyMap(mapSize)
        val origMap = AtomicMutableMap(initialMap)
        val offset = if (keysIntersect) mapSize - 5 else mapSize
        val additionalMap = randomIntKeyMap(size = 10, offset = offset)

        origMap.putAll(additionalMap)

        for ((key, value) in additionalMap) {
            assertTrue(origMap.containsKey(key))
            assertEquals(value, origMap[key])
        }
        assertEquals(mapSize + additionalMap.size - (mapSize - offset), origMap.size)
    }

    @Test
    fun `put should add item and return null when key does not already exist`() {
        val mapSize = 100
        val initialMap = randomIntKeyMap(mapSize)
        val origMap = AtomicMutableMap(initialMap)
        val putKey = 1000
        val putValue = "Put Value"

        val result = origMap.put(putKey, putValue)

        assertNull(result)

        val item = origMap[putKey]
        assertNotNull(item)
        assertEquals(putValue, item)
        assertEquals(mapSize + 1, origMap.size)
    }

    @Test
    fun `put should add item and return existing value when key already exists`() {
        val mapSize = 100
        val initialMap = randomIntKeyMap(mapSize)
        val origMap = AtomicMutableMap(initialMap)
        val putKey = 100
        val putValue = "Put Value"

        val result = origMap.put(putKey, putValue)

        assertNotNull(result)

        val item = origMap[putKey]
        assertNotNull(item)
        assertEquals(putValue, item)
        assertEquals(mapSize, origMap.size)
    }
}
