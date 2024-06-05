package io.github.clasicrando.kdbc.postgresql.copy

interface PgCsvCopyRow {
    val values: List<Any?>
}
