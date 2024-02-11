package com.github.clasicrando.postgresql.column

fun interface PgTypeDecoder<out T> {
    fun decode(value: PgValue): T
}
