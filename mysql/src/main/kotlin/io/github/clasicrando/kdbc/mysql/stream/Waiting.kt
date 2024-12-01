package io.github.clasicrando.kdbc.mysql.stream

/**
 * Waiting status of a [MySqlStream]. The stream is either waiting for another result or the next
 * row.
 */
internal enum class Waiting {
    Result,
    Row,
}
