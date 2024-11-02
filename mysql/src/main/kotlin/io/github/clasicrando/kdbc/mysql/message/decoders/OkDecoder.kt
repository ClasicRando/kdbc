package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status
import io.github.clasicrando.kdbc.mysql.readLongLengthEncoded

internal object OkDecoder : MessageDecoder<MysqlMessage.Ok, Unit> {
    @OptIn(ExperimentalStdlibApi::class)
    override fun decode(
        buffer: ByteReadBuffer,
        context: Unit,
    ): MysqlMessage.Ok {
        val header = buffer.readByteAsInt()
        checkOrKdbcException(header == 0 || header == 0xFE) {
            "Expected Ok header (0x00 or 0xFE) but found ${header.toHexString()}"
        }

        val affectedRows = buffer.readLongLengthEncoded()
        val lastInsertId = buffer.readLongLengthEncoded()
        val status = Status(buffer.readShortLe())
        val warnings = buffer.readShortLe().toInt()

        return MysqlMessage.Ok(affectedRows, lastInsertId, status, warnings)
    }
}
