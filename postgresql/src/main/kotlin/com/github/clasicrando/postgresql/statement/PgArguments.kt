package com.github.clasicrando.postgresql.statement

import com.github.clasicrando.common.buffer.ArrayListBuffer
import com.github.clasicrando.common.buffer.writeLengthPrefixed
import com.github.clasicrando.postgresql.column.PgType
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.type.PgJson
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeFully
import io.ktor.utils.io.core.writeShort

class PgArguments internal constructor(
    private val typeRegistry: PgTypeRegistry,
    private val parameters: List<Any?>,
    private val statement: PgPreparedStatement,
) : ArrayListBuffer() {
    fun writeToBuffer(buffer: BytePacketBuilder) {
        buffer.writeShort(parameters.size.toShort())
        for (parameterIndex in parameters.indices) {
            val parameter = parameters[parameterIndex]
            if (parameter is PgJson) {
                statement.parameterTypeOids.getOrNull(parameterIndex)?.let { oid ->
                    if (oid == PgType.JSON || oid == PgType.JSON_ARRAY) {
                        writeByte(' '.code.toByte())
                    } else {
                        writeByte(1)
                    }
                }
            }
            writeLengthPrefixed {
                typeRegistry.encode(parameter, it)
            }
        }
        buffer.writeFully(this.toByteArray())
    }
}
