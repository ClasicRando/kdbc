package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object ErrDecoder : MessageDecoder<MysqlMessage.Err, Capabilities> {
    @OptIn(ExperimentalStdlibApi::class)
    override fun decode(
        buffer: ByteReadBuffer,
        context: Capabilities,
    ): MysqlMessage.Err {
        val header = buffer.readByteAsInt()
        checkOrKdbcException(header == 0xFF) {
            "Expected Err header (0xFF) but found ${header.toHexString()}"
        }

        val errorCode = buffer.readShortLe().toInt()
        var sqlState: String? = null

        if (context[Capabilities.CLIENT_PROTOCOL_41]) {
            if (buffer.peekNext() == '#'.code.toByte()) {
                buffer.readByte()
                sqlState = buffer.readBytes(5).toString(Charsets.UTF_8)
            }
        }

        val errorMessage = buffer.readText()
        return MysqlMessage.Err(errorCode, sqlState, errorMessage)
    }
}
