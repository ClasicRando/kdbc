package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.buffer.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status
import kotlinx.io.Source
import kotlinx.io.readShortLe

internal object OkDecoder : MessageDecoder<MysqlMessage.Ok, Unit> {
    override fun decode(buffer: Source, context: Unit): MysqlMessage.Ok {
        val header = buffer.readByteAsInt()
        checkOrKdbcException(header == 0 || header == 0xFE) {
            "Expected Ok header (0x00 or 0xFE) but found 0x${header.toHexString()}"
        }

        val affectedRows = buffer.readLongLengthEncoded()
        val lastInsertId = buffer.readLongLengthEncoded()
        val status = Status(buffer.readShortLe())
        val warnings = buffer.readShortLe().toInt()

        return MysqlMessage.Ok(affectedRows, lastInsertId, status, warnings)
    }

    fun decode(buffer: Source): MysqlMessage.Ok {
        return decode(buffer, Unit)
    }
}
