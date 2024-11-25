package io.github.clasicrando.kdbc.core.cache

public class LruCache<K, V>(public val capacity: Int) {
    private var removedEntry: Map.Entry<K, V>? = null
    private val map: LinkedHashMap<K, V> = object : LinkedHashMap<K, V>(capacity) {
        override fun removeEldestEntry(eldest: Map.Entry<K, V>): Boolean {
            if (size > capacity) {
                removedEntry = eldest
                return true
            }
            return false
        }
    }

    public operator fun get(key: K): V? = map[key]

    /**
     * Add the new [key] and [value] into the cache. Returns any entry that was removed to add this
     * entry. The removed entry will always be the last inserted entry.
     */
    public fun insert(key: K, value: V): Map.Entry<K, V>? {
        map[key] = value
        return removedEntry
    }

    public fun clear() {
        map.clear()
    }
}
