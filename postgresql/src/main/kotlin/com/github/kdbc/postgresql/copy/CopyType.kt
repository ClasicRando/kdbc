package com.github.kdbc.postgresql.copy

/**
 * Direction of the copy operation. This matches the `COPY` statement variants only since the copy
 * spec also includes `COPY BOTH` as a valid operation for streaming replication.
 */
enum class CopyType {
    From,
    To,
}
