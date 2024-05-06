package io.github.clasicrando.kdbc.postgresql.type

sealed interface Bound<T> {
    data class Included<T>(val value: T) : Bound<T>
    data class Excluded<T>(val value: T): Bound<T>
    class Unbounded<T> : Bound<T>
}

data class PgRange<T : Any>(
    val start: Bound<T>,
    val end: Bound<T>,
)
