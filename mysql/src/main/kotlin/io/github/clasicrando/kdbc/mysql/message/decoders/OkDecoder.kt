package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.readByteAsInt
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.buffer.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.exceptions.checkOrMySqlException
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status
import kotlinx.io.Buffer
import kotlinx.io.readShortLe

/**
 * [MessageDecoder] for [MysqlMessage.Ok] packets. Starts with 0x00 or 0xfe, followed by the
 * affected row count (if any), the last insert ID (if any), the server status and the number of
 * warnings.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html)
 */
internal object OkDecoder : MessageDecoder<MysqlMessage.Ok, Unit> {
    override fun decode(buffer: Buffer, context: Unit): MysqlMessage.Ok {
        val header = buffer.readByteAsInt()
        checkOrMySqlException(header == 0x00 || header == 0xfe) {
            "Expected Ok header (0x00 or 0xfe) but found 0x${header.toHexString()}"
        }

        val affectedRows = buffer.readLongLengthEncoded()
        val lastInsertId = buffer.readLongLengthEncoded()
        val status = Status(buffer.readShortLe())
        val warnings = buffer.readShortLe().toInt()

        return MysqlMessage.Ok(affectedRows, lastInsertId, status, warnings)
    }

    fun decode(buffer: Buffer): MysqlMessage.Ok {
        return decode(buffer, Unit)
    }
}
