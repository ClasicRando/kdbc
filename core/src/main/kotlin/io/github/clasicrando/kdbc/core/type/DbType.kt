package io.github.clasicrando.kdbc.core.type

import kotlin.reflect.KType

interface DbType<T : Any, in V : Any, D : Any> : Encode<T>, Decode<T, V> {
    val dbType: D

    val kType: KType

    fun isCompatible(dbType: D): Boolean

    fun getActualType(value: T): D
}
