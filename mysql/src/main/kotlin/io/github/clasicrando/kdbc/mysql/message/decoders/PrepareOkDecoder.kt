package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Source
import kotlinx.io.readIntLe
import kotlinx.io.readShortLe

internal object PrepareOkDecoder : MessageDecoder<MysqlMessage.PrepareOk, Capabilities> {
    override fun decode(
        buffer: Source,
        context: Capabilities,
    ): MysqlMessage.PrepareOk {
        val status = buffer.readByteAsInt()
        checkOrKdbcException(status == 0) {
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
