package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object PrepareOkDecoder : MessageDecoder<MysqlMessage.PrepareOk, Unit> {
    @OptIn(ExperimentalStdlibApi::class)
    override fun decode(
        buffer: ByteReadBuffer,
        context: Unit,
    ): MysqlMessage.PrepareOk {
        val status = buffer.readByte()
        checkOrKdbcException(status == 0.toByte()) {
            "Expected PrepareOk status to be 0x00 but found ${status.toHexString()}"
        }

        val statementId = buffer.readIntLe()
        val columns = buffer.readShortLe().toInt()
        val params = buffer.readShortLe().toInt()
        buffer.readByte()
        val warnings = buffer.readShortLe().toInt()

        return MysqlMessage.PrepareOk(statementId, columns, params, warnings)
    }
}
