package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.readByteAsInt
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.exceptions.checkOrMySqlException
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status
import kotlinx.io.Buffer
import kotlinx.io.readShortLe

/**
 * [MessageDecoder] for [MysqlMessage.Eof] packets. Starts with a 0xfe header followed by the number
 * of warnings (as a [Short]) and then the [Status] flags.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html)
 */
internal object EofDecoder : MessageDecoder<MysqlMessage.Eof, Unit> {
    override fun decode(buffer: Buffer, context: Unit): MysqlMessage.Eof {
        val header = buffer.readByteAsInt()
        checkOrMySqlException(header == 0xfe) {
            "Expected auth switch header (0xfe) but found 0x${header.toHexString()}"
        }

        val warnings = buffer.readShortLe().toInt()
        val status = Status(buffer.readShortLe())
        return MysqlMessage.Eof(warnings, status)
    }

    fun decode(buffer: Buffer): MysqlMessage.Eof {
        return decode(buffer, Unit)
    }
}
