package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.readBytesLengthEncoded
import io.github.clasicrando.kdbc.mysql.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.result.ColumnFlags
import io.github.clasicrando.kdbc.mysql.type.MySqlType

internal object ColumnDefinitionDecoder : MessageDecoder<MysqlMessage.ColumnDefinition, Unit> {
    override fun decode(
        buffer: ByteReadBuffer,
        context: Unit,
    ): MysqlMessage.ColumnDefinition {
        val catalog = buffer.readBytesLengthEncoded()
        val schema = buffer.readBytesLengthEncoded()
        val tableAlias = buffer.readBytesLengthEncoded()
        val table = buffer.readBytesLengthEncoded()
        val alias = buffer.readBytesLengthEncoded()
        val name = buffer.readBytesLengthEncoded()
        buffer.readLongLengthEncoded() // next length, always 0x0c
        val collation = buffer.readShortLe().toInt()
        val maxSize = buffer.readIntLe()
        val typeId = buffer.readByteAsInt()
        val flags = buffer.readShortLe().toInt() and 0xffff
        val decimals = buffer.readByte()
        return MysqlMessage.ColumnDefinition(
            catalog = catalog,
            schema = schema,
            tableAlias = tableAlias,
            table = table,
            alias = alias,
            name = name,
            collation = collation,
            maxSize = maxSize,
            type = MySqlType.from(typeId),
            flags = ColumnFlags(flags),
            decimals = decimals,
        )
    }
}
