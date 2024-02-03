package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent variable length character arrays. This includes text,
 * varchar, char, nvarchar, nchar and other assorted database types. Values are decoded as string
 * and encoded by simply passing forward the [String] reference.
 */
object StringDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value
    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is CharArray -> buffer.writeText(value, charset = charset)
            is CharSequence -> buffer.writeText(value, charset = charset)
            else -> buffer.writeText(value.toString(), charset = charset)
        }
    }

    override val encodeType: KClass<*> = String::class
}
