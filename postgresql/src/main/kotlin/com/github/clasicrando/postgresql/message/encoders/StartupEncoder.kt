package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.StartupMessage]. This message is sent to initiate the startup of
 * a postgresql connection. The contents are:
 * - the length of the following data (including the size of the [Int] length)
 * - the protocol version split into 2 [Short] values
 *     - the major version (only major version 3 is supported)
 *     - the minor version (only minor version 0 is supported)
 * - zero or more key value pairs of startup parameters
 *     - key = CString
 *     - value = CString
 * - zero [Byte] terminator to signify the end of the parameters
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-CLOSE)
 */
internal object StartupEncoder : MessageEncoder<PgMessage.StartupMessage> {
    override fun encode(value: PgMessage.StartupMessage, buffer: ByteWriteBuffer) {
        buffer.writeLengthPrefixed(includeLength = true) {
            writeShort(3)
            writeShort(0)
            for ((k, v) in value.params) {
                writeCString(k)
                writeCString(v)
            }
            writeByte(0)
        }
    }
}