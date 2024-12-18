package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.peekNextAsInt
import io.github.clasicrando.kdbc.core.buffer.readByteAsInt
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.exceptions.checkOrMySqlException
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Buffer
import kotlinx.io.readShortLe
import kotlinx.io.readString

/**
 * [MessageDecoder] for [MysqlMessage.Err] packets. Starts with 0xfe followed by the error code, the
 * SQL state code and finally the error message (end of packet terminated).
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html)
 */
internal object ErrDecoder : MessageDecoder<MysqlMessage.Err, Capabilities> {
    override fun decode(buffer: Buffer, context: Capabilities): MysqlMessage.Err {
        val header = buffer.readByteAsInt()
        checkOrMySqlException(header == 0xff) {
            "Expected Err header (0xff) but found 0x${header.toHexString()}"
        }

        val errorCode = buffer.readShortLe().toInt()
        var sqlState: String? = null

        if (context[Capabilities.CLIENT_PROTOCOL_41]) {
            if (buffer.peekNextAsInt() == '#'.code) {
                buffer.readByte()
                sqlState = buffer.readString(byteCount = 5)
            }
        }

        val errorMessage = buffer.readString()
        return MysqlMessage.Err(errorCode, sqlState, errorMessage)
    }
}
