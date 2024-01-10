package com.github.clasicrando.postgresql.array

import com.github.clasicrando.common.column.ColumnInfo
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnDecodeError
import io.ktor.utils.io.charsets.Charset
import kotlin.reflect.KClass

/**
 * [DbType] for Postgresql arrays of any [innerDbType]. Allows for decoding array literals into
 * a [List] of the [innerDbType]'s [DbType.encodeType].
 *
 * Note that this class does not encode lists into array literals (that is handled by the
 * postgresql [TypeRegistry][com.github.clasicrando.common.column.TypeRegistry]) and string
 * decoding is not supported (the bytes are converted to a string and the [Charset] is used to
 * decode the [innerDbType] is string decoding is not supported).
 */
class PgArrayType(private val innerDbType: DbType) : DbType {

    override val supportsStringDecoding: Boolean get() = false

    override fun decode(type: ColumnInfo, bytes: ByteArray, charset: Charset): Any {
        val literal = String(bytes, charset = charset)
        if (!literal.startsWith('{') || !literal.endsWith('}')) {
            columnDecodeError(type, bytes)
        }

        return ArrayLiteralParser.parse(literal).map {
            when {
                it == null -> null
                innerDbType.supportsStringDecoding -> innerDbType.decode(type, it)
                else -> innerDbType.decode(type, it.toByteArray(charset = charset), charset)
            }
        }.toList()
    }

    override fun decode(type: ColumnInfo, value: String): Any {
        throw UnsupportedOperationException("Attempted to decode an array type as string")
    }

    override val encodeType: KClass<*> = List::class

    override fun encode(value: Any): String {
        throw NotImplementedError("Array encoding is not handled in each type instance")
    }
}
