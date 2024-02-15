package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.column.PgType
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.row.PgColumnDescription
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readInt
import io.ktor.utils.io.core.readShort

internal object RowDescriptionDecoder : MessageDecoder<PgMessage.RowDescription> {
    override fun decode(packet: ByteReadPacket): PgMessage.RowDescription {
        val columnsCount = packet.readShort()
        val descriptions = (0..<columnsCount).map {
            PgColumnDescription(
                fieldName = packet.readCString(),
                tableOid = packet.readInt(),
                columnAttribute = packet.readShort(),
                pgType = PgType.fromOid(packet.readInt()),
                dataTypeSize = packet.readShort(),
                typeModifier = packet.readInt(),
                formatCode = packet.readShort(),
            )
        }
        return PgMessage.RowDescription(descriptions)
    }
}
