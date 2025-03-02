package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.result.ColumnFlags
import kotlinx.io.Sink
import kotlinx.io.writeIntLe

/**
 * [MessageEncoder] for [MysqlMessage.Execute]. Executes a prepared statement with the given bound
 * parameters.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html)
 */
internal object ExecuteEncoder : MessageEncoder<MysqlMessage.Execute, LongBitFlags> {
    override fun encode(value: MysqlMessage.Execute, buffer: Sink, context: LongBitFlags) {
        buffer.writeByte(0x17)
        buffer.writeIntLe(value.statement)
        buffer.writeByte(0)
        buffer.writeIntLe(1)

        if (value.arguments.inner.isNotEmpty()) {
            val arguments = value.arguments
            val innerArgs = arguments.inner
            buffer.write(arguments.nullBitMap)
            buffer.writeByte(1)

            for (i in innerArgs.indices) {
                val arg = innerArgs[i]
                buffer.writeByte(arg.typeDescription.dbType.inner.toByte())
                buffer.writeByte(
                    if (arg.typeDescription.flags[ColumnFlags.UNSIGNED]) {
                        0x80.toByte()
                    } else {
                        0
                    }
                )
            }
            for (i in innerArgs.indices) {
                val arg = innerArgs[i]
                val argValue = arg.value
                if (argValue != null) {
                    arg.typeDescription.encode(argValue, buffer)
                }
            }
        }
    }
}
