package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Source
import kotlinx.io.readByteArray
import kotlinx.io.readShortLe
import kotlinx.io.readString

internal object ErrDecoder : MessageDecoder<MysqlMessage.Err, Capabilities> {
    override fun decode(
        buffer: Source,
        context: Capabilities,
    ): MysqlMessage.Err {
        val header = buffer.readByteAsInt()
        checkOrKdbcException(header == 0xFF) {
            "Expected Err header (0xFF) but found 0x${header.toHexString()}"
        }

        val errorCode = buffer.readShortLe().toInt()
        var sqlState: String? = null

        if (context[Capabilities.CLIENT_PROTOCOL_41]) {
            if (buffer.peek().readByte() == '#'.code.toByte()) {
                buffer.readByte()
                sqlState = buffer.readByteArray(5).toString(Charsets.UTF_8)
            }
        }

        val errorMessage = buffer.readString()
        return MysqlMessage.Err(errorCode, sqlState, errorMessage)
    }
}
