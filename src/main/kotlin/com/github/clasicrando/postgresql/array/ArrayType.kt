package com.github.clasicrando.postgresql.array

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnDecodeError
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer
import kotlin.reflect.KClass

class ArrayType(private val innerDbType: DbType) : DbType {

    override val supportsStringDecoding: Boolean get() = false

    override fun decode(type: ColumnData, bytes: ByteArray, charset: Charset): Any {
        val literal = String(bytes, charset = charset)
        if (!literal.startsWith('{') || !literal.endsWith('}')) {
            columnDecodeError(type, bytes)
        }

        return ArrayLiteralParser.parse(literal).map {
            if (it == null) {
                return@map null
            }
            if (innerDbType.supportsStringDecoding) {
                innerDbType.decode(type, it)
            } else {
                innerDbType.decode(type, it.toByteArray(charset = charset), charset)
            }
        }.toList()
    }

    override fun decode(type: ColumnData, value: String): Any {
        throw UnsupportedOperationException("Attempted to decode an array type as string")
    }

    override val encodeType: KClass<*> = List::class

    override fun encode(value: Any): String {
        throw NotImplementedError("Array encoding is not handled in each type instance")
    }
}
