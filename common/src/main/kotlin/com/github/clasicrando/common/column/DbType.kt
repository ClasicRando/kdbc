package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import kotlin.reflect.KClass

/**
 * Specification for a Database type. Describes how a database's type is decoded from raw bytes (and
 * the string representation if [supportsStringDecoding] is true) as well as how the subsequent
 * kotlin type is encoded, so it can be sent to the database.
 */
interface DbType {
    /**
     * Decode the [bytes] supplied as the [type] specified by the database row description.
     *
     * The default implementation converts the bytes to a string using the [charset] specified,
     * passing that to the string parameter [decode] method.
     */
    fun decode(type: ColumnData, bytes: ByteArray, charset: Charset): Any {
        return decode(type, String(bytes, charset))
    }

    /** Decode the string [value] supplied as the [type] specified by the database */
    fun decode(type: ColumnData, value: String): Any

    /**
     * Encode the provided [value] into a string. Must be in a format the database expects a string
     * literal to be in for the required type. For instance, a datetime type must be in a specific
     * format to be read by the database correctly.
     *
     * The default implementation simply calls the [Any.toString] method so custom types can simply
     * override that method to not require custom step within the corresponding [DbType].
     */
    fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder)

    /** Kotlin type mapped to the database type */
    val encodeType: KClass<*>

    /** Signifies that a type can be decoded from the bytes as a string (default = true) */
    val supportsStringDecoding: Boolean get() = true
}
