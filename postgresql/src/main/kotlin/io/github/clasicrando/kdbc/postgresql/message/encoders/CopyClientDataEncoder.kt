package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Sink

/**
 * [MessageEncoder] for [PgMessage.CopyClientData]. This message is sent to pass bytes as row data
 * in a `COPY FROM` operation. The contents are:
 * - a header [Byte] of 'd'
 * - the length of the following data (including the size of the [Int] length)
 * - a chunk of row data as a [ByteArray]
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYDATA)
 */
internal object CopyClientDataEncoder : PgMessageEncoder<PgMessage.CopyClientData>() {
    override fun encode(value: PgMessage.CopyClientData, buffer: Sink) {
        buffer.writeByte(value.code)
        buffer.writeInt(value.data.size + 4)
        buffer.write(value.data)
    }
}
