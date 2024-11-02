package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status

internal object EofDecoder : MessageDecoder<MysqlMessage.Eof, Unit> {
    @OptIn(ExperimentalStdlibApi::class)
    override fun decode(
        buffer: ByteReadBuffer,
        context: Unit,
    ): MysqlMessage.Eof {
        val header = buffer.readByteAsInt()
        checkOrKdbcException(header == 0xFE) {
            "Expected auth switch header (0xFE) but found ${header.toHexString()}"
        }

        val warnings = buffer.readShortLe().toInt()
        val status = Status(buffer.readShortLe())
        return MysqlMessage.Eof(warnings, status)
    }
}
