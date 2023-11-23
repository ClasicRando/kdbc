package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.row.PgRowFieldDescription
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readInt
import io.ktor.utils.io.core.readShort

class RowDescriptionDecoder(
    private val charset: Charset,
) : MessageDecoder<PgMessage.RowDescription> {
    override fun decode(packet: ByteReadPacket): PgMessage.RowDescription {
        val columnsCount = packet.readShort()
        val descriptions = (0..<columnsCount).map {
            PgRowFieldDescription(
                fieldName = packet.readCString(charset),
                tableOid = packet.readInt(),
                columnAttribute = packet.readShort(),
                dataTypeOid = packet.readInt(),
                dataTypeSize = packet.readShort(),
                typeModifier = packet.readInt(),
                formatCode = packet.readShort(),
            )
        }
        return PgMessage.RowDescription(descriptions)
    }
}
