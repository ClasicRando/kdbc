package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.postgresql.exceptions.PgException

/**
 * Format code for Postgres values. Currently only text and binary values are supported
 */
public enum class PgFormatCode(internal val code: Short) {
    Text(0),
    Binary(1);

    internal companion object {
        /**
         * Decode the [value] as a [PgFormatCode]. Throws a [PgException] when the value is not 0
         * or 1.
         */
        fun fromShort(value: Short): PgFormatCode {
            return when (value) {
                0.toShort() -> Text
                1.toShort() -> Binary
                else -> throw PgException("Invalid format code from row description. Got $value")
            }
        }
    }
}
