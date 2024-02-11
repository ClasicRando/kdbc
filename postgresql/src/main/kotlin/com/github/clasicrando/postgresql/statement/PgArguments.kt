package com.github.clasicrando.postgresql.statement

import com.github.clasicrando.postgresql.column.PgType
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.message.encoders.writeLengthPrefixedWithoutSelf
import com.github.clasicrando.postgresql.type.PgJson
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeShort

internal class PgArguments(
    private val typeRegistry: PgTypeRegistry,
    private val parameters: List<Any?>,
    private val statement: PgPreparedStatement,
) {
    val paramCount = parameters.size

    fun writeToBuffer(buffer: BytePacketBuilder) {
        buffer.writeShort(paramCount.toShort())
        for ((value, oid) in parameters.zip(statement.parameterTypeOids)) {
            buffer.writeLengthPrefixedWithoutSelf {
                if (value is PgJson) {
                    if (oid == PgType.JSON || oid == PgType.JSON_ARRAY) {
                        this.writeByte(' '.code.toByte())
                    } else {
                        this.writeByte(1)
                    }
                }
                typeRegistry.encode(value, this)
            }
        }
    }
}
