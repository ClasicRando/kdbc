package com.github.clasicrando.postgresql.statement

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.postgresql.column.PgType
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.type.PgJson

class PgArguments internal constructor(
    private val typeRegistry: PgTypeRegistry,
    private val parameters: List<Any?>,
    private val statement: PgPreparedStatement,
) {
    fun writeToBuffer(buffer: ByteWriteBuffer) {
        buffer.writeShort(parameters.size.toShort())
        for (parameterIndex in parameters.indices) {
            val parameter = parameters[parameterIndex]
            if (parameter == null) {
                buffer.writeInt(-1)
                continue
            }
            if (parameter is PgJson) {
                statement.parameterTypeOids.getOrNull(parameterIndex)?.let { oid ->
                    if (oid == PgType.JSON || oid == PgType.JSON_ARRAY) {
                        buffer.writeByte(' '.code.toByte())
                    } else {
                        buffer.writeByte(1)
                    }
                }
            }
            buffer.writeLengthPrefixed {
                typeRegistry.encode(parameter, this)
            }
        }
    }
}
