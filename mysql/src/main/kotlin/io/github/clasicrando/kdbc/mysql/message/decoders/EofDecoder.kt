package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status
import kotlinx.io.Source
import kotlinx.io.readShortLe

internal object EofDecoder : MessageDecoder<MysqlMessage.Eof, Unit> {
    override fun decode(buffer: Source, context: Unit): MysqlMessage.Eof {
        val header = buffer.readByteAsInt()
        checkOrKdbcException(header == 0xFE) {
            "Expected auth switch header (0xFE) but found 0x${header.toHexString()}"
        }

        val warnings = buffer.readShortLe().toInt()
        val status = Status(buffer.readShortLe())
        return MysqlMessage.Eof(warnings, status)
    }

    fun decode(buffer: Source): MysqlMessage.Eof {
        return decode(buffer, Unit)
    }
}
