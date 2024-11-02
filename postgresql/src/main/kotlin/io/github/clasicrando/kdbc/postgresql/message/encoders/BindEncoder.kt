package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.writeCString
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.buffer.writeLengthPrefixedInt
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.statement.encodeValue
import kotlinx.io.Sink

/**
 * [MessageEncoder] for [PgMessage.Bind]. This message is sent to initiate the backend to bind
 * parameters to an already prepared statement. Contents are:
 * - a header [Byte] of 'B'
 * - the length of the following data (including the size of the [Int] length)
 * - a CString as the portal name (an empty string is the portal name is null)
 * - a CString as the statement name
 * - a [Short] as the number of parameter format codes (always 1 since we use binary encoding only)
 * - a [Short] as the format code (1 = binary)
 * - all the parameters written as sequential pairs of:
 *     - [Int], the length of the parameter values as bytes
 *     - [ByteArray], the value of the parameter encoded into bytes
 * - a [Short] as the number of result column format codes (always 1 since we only use binary
 *   encoding)
 * - a [Short] as the format code of the result columns (1 = binary)
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BIND)
 */
internal object BindEncoder : MessageEncoder<PgMessage.Bind> {
    override fun encode(value: PgMessage.Bind, buffer: Sink) {
        buffer.writeByte(value.code)
        buffer.writeLengthPrefixedInt(includeLength = true) {
            writeCString(value.portal ?: "")
            writeCString(value.statementName)
            writeShort(1)
            writeShort(1)
            writeShort(value.parameters.size.toShort())
            for ((parameter, type) in value.parameters) {
                this.encodeValue(parameter, type, value.typeCache)
            }
            writeShort(1)
            writeShort(1)
        }
    }
}
