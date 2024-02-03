package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlinx.uuid.UUID
import kotlin.reflect.KClass
/**
 * [DbType] for database types that represent
 * [UUID](https://en.wikipedia.org/wiki/Universally_unique_identifier) values. Values as encoded
 * and decoded as strings. Although it is easy to convert the decoded value to a java uuid, the
 * actual type returned is a kotlinx variant for multi-platform support.
 */
object UuidDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = UUID(value)

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is UUID -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<UUID>(value)
        }
    }

    override val encodeType: KClass<*> = UUID::class
}
