package com.github.kdbc.postgresql.column

import com.github.kdbc.core.column.ColumnDecodeError

/**
 * Interface defining a type that decodes [PgValue]s into the required output type [T]. This
 * interface should not be inherited by an instantiable class but rather an object to define
 * decoders only once and reuse instance since the object should hold no state and [decode] is a
 * pure function.
 */
fun interface PgTypeDecoder<out T> {
    /**
     * Use the data and context within [value] to return a new instance of [T]. This method should
     * generally have 2 paths:
     *
     * 1. Decode [PgValue.Binary] using the bytes view within
     * 2. Decode [PgValue.Text] using the [String] within
     *
     * In some cases the 2 cases can converge into a single branch of logic. For example, [Enum]
     * types are always text so the binary version is just a [String] encoded using the client
     * encoding.
     *
     * @throws ColumnDecodeError if the decode operation fails. In most cases, other exceptions
     * will be caught and this [Exception] will be thrown instead to give more context as to why
     * the operation failed.
     */
    fun decode(value: PgValue): T
}
