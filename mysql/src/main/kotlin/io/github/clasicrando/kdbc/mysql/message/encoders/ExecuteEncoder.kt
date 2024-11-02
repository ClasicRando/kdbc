package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeIntLe
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.result.ColumnFlags

internal object ExecuteEncoder : MessageEncoder<MysqlMessage.Execute, Capabilities> {
    override fun encode(
        value: MysqlMessage.Execute,
        buffer: ByteWriteBuffer,
        context: Capabilities,
    ) {
        buffer.writeByte(0x17)
        buffer.writeIntLe(value.statement)
        buffer.writeByte(0)
        buffer.writeIntLe(1)

        if (!value.arguments.isEmpty) {
            val arguments = value.arguments
            buffer.writeBytes(arguments.nullBitMap())
            buffer.writeByte(1)

            for (i in arguments.inner.indices) {
                val arg = arguments.inner[i]
                buffer.writeByte(
                    arg.typeInfo.type.inner
                        .toByte(),
                )
                buffer.writeByte(
                    if (arg.typeInfo.flags[ColumnFlags.UNSIGNED]) {
                        0x80.toByte()
                    } else {
                        0
                    },
                )
            }
            for (i in arguments.inner.indices) {
                val arg = arguments.inner[i]
                val argValue = arg.value.value
                if (argValue != null) {
                    arg.typeDescription.encode(argValue, buffer)
                }
            }
        }
    }
}
