package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.buffer.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.result.MySqlColumn
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import io.github.clasicrando.kdbc.mysql.type.MySqlType
import kotlinx.io.Source
import kotlinx.io.readByteArray

internal object BinaryRowDecoder : MessageDecoder<MysqlMessage.BinaryRow, List<MySqlColumn>> {
    override fun decode(buffer: Source, context: List<MySqlColumn>): MysqlMessage.BinaryRow {
        val header = buffer.readByte()
        checkOrKdbcException(header == 0.toByte()) {
            "Expected row header (0x00) but found 0x${header.toHexString()}"
        }

        val nullBitmap = buffer.readByteArray((context.size + 9) / 8)

        val values: Array<MySqlValue?> =
            Array(context.size) { i ->
                val columnNullIndex = i + 2

                val byteIndex = columnNullIndex / 8
                val bitIndex = columnNullIndex % 8

                val isNull = (nullBitmap[byteIndex].toInt() and 0xFF and (1 shl bitIndex)) != 0
                if (isNull) {
                    return@Array null
                }

                val column = context[i]
                val size =
                    when (column.typeInfo.type) {
                        MySqlType.LongLong,
                        MySqlType.Double -> 8
                        MySqlType.Long,
                        MySqlType.Int24,
                        MySqlType.Float -> 4
                        MySqlType.Short,
                        MySqlType.Year -> 2
                        MySqlType.Tiny -> 1
                        MySqlType.String,
                        MySqlType.Varchar,
                        MySqlType.VarString,
                        MySqlType.Enum,
                        MySqlType.LongBlob,
                        MySqlType.Set,
                        MySqlType.MediumBlob,
                        MySqlType.Blob,
                        MySqlType.TinyBlob,
                        MySqlType.Geometry,
                        MySqlType.Bit,
                        MySqlType.Decimal,
                        MySqlType.Json,
                        MySqlType.NewDecimal -> buffer.readLongLengthEncoded().toInt()
                        MySqlType.Time,
                        MySqlType.Timestamp,
                        MySqlType.Date,
                        MySqlType.Datetime -> buffer.readByteAsInt() + 1
                        MySqlType.Null ->
                            throw KdbcException("Unreachable! Found null type for non-null value")
                    }

                MySqlValue.Binary(ByteReadBuffer(buffer.readByteArray(size)), context[i])
            }
        return MysqlMessage.BinaryRow(values)
    }
}
