package com.github.clasicrando.postgresql.type

@JvmInline
value class PgOid(val value: Long) {
    override fun toString(): String {
        return value.toString()
    }
}
