package io.github.clasicrando.kdbc.core.type

import kotlin.reflect.KType

public interface DbType<T : Any, in V : Any, D : Any> : Encode<T>, Decode<T, V> {
    public val dbType: D

    public val kType: KType

    public fun isCompatible(dbType: D): Boolean

    public fun getActualType(value: T): D
}
