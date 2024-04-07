package com.github.kdbc.postgresql.statement

import com.github.kdbc.core.buffer.ByteWriteBuffer
import com.github.kdbc.postgresql.column.PgType
import com.github.kdbc.postgresql.column.PgTypeRegistry
import com.github.kdbc.postgresql.type.PgJson

/**
 * Container for [parameters] to be bound to a portal created from a prepared [statement]. Also
 * keeps reference to a [typeRegistry] to encode the value into a [ByteWriteBuffer] supplied to
 * the [writeToBuffer] method.
 */
class PgArguments internal constructor(
    private val typeRegistry: PgTypeRegistry,
    private val parameters: List<Any?>,
    private val statement: PgPreparedStatement,
) {
    /**
     * Write the [parameters] and required metadata to the supplied [buffer]. Writes:
     *
     * - a [Short] as the number of parameters
     * - for each parameter:
     *     - the length of the parameter's encoded value (or -1 if the parameter is null)
     *     - the parameter's encoded value using the encode method on [typeRegistry] (or nothing if
     *     the parameter is null)
     *
     * As a special case, the [PgType.Jsonb] type requires a version number [Byte] before the
     * encoded value. That means if the parameter is of type [PgJson], then the parameter's type in
     * the [statement] is checked to find out if the expected data type OID [PgType.JSONB]/
     * [PgType.JSONB_ARRAY]. If the data type matches either, the version [Byte] is added before
     * the encoded value.
     */
    fun writeToBuffer(buffer: ByteWriteBuffer) {
        buffer.writeShort(parameters.size.toShort())
        for (parameterIndex in parameters.indices) {
            val parameter = parameters[parameterIndex]
            if (parameter == null) {
                buffer.writeInt(-1)
                continue
            }
            buffer.writeLengthPrefixed {
                if (parameter is PgJson) {
                    statement.parameterTypeOids.getOrNull(parameterIndex)?.let { oid ->
                        if (oid == PgType.JSONB || oid == PgType.JSONB_ARRAY) {
                            buffer.writeByte(1)
                        }
                    }
                }
                typeRegistry.encode(parameter, this)
            }
        }
    }
}
