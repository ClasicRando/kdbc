package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.readByteAsInt
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.exceptions.checkOrMySqlException
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Buffer
import kotlinx.io.readIntLe
import kotlinx.io.readShortLe

/**
 * [MessageDecoder] for [MysqlMessage.PrepareOk] packets. Starts with 0x00 followed by the statement
 * ID, number of columns, number of parameters, an empty byte, and finally the number of warnings.
 * This message is always followed by the parameter and column definitions if the parameter or
 * column counts are not zero.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response_ok)
 */
internal object PrepareOkDecoder : MessageDecoder<MysqlMessage.PrepareOk, Capabilities> {
    override fun decode(buffer: Buffer, context: Capabilities): MysqlMessage.PrepareOk {
        val status = buffer.readByteAsInt()
        checkOrMySqlException(status == 0x00) {
            "Expected PrepareOk status to be 0x00 but found 0x${status.toHexString()}"
        }

        val statementId = buffer.readIntLe()
        val columns = buffer.readShortLe().toInt()
        val params = buffer.readShortLe().toInt()
        buffer.readByte()
        val warnings = buffer.readShortLe().toInt()

        return MysqlMessage.PrepareOk(statementId, columns, params, warnings)
    }
}
