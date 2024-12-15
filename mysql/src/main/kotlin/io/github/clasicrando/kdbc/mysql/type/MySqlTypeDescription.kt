package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.connection.IntBitFlags
import io.github.clasicrando.kdbc.core.type.DbType
import io.github.clasicrando.kdbc.mysql.result.ColumnFlags
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import kotlin.reflect.KType

public abstract class MySqlTypeDescription<T : Any>(
    /**
     * [MySqlType] that is referenced for this type description as the serialization input and
     * deserialization output
     */
    final override val dbType: MySqlType,
    /** Kotlin type of [T] that is recognized by this type description */
    final override val kType: KType,
) : DbType<T, MySqlValue, MySqlType> {
    public open val flags: IntBitFlags = ColumnFlags.BINARY

    /** Decode the bytes provided into the type [T] */
    public abstract fun decodeBytes(value: MySqlValue.Binary): T

    /** Decode the [String] provided into the type [T] */
    public abstract fun decodeText(value: MySqlValue.Text): T

    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == this.dbType
    }

    override fun getActualType(value: T): MySqlType {
        return dbType
    }

    final override fun decode(value: MySqlValue): T {
        return try {
            when (value) {
                is MySqlValue.Binary -> decodeBytes(value)
                is MySqlValue.Text -> decodeText(value)
            }
        } catch (ex: ColumnDecodeError) {
            throw ex
        } catch (ex: Exception) {
            columnDecodeError(
                kType = kType,
                type = value.column,
                reason = "Failed to decode bytes. See cause",
                cause = ex,
            )
        } finally {
            value.bytes.reset()
        }
    }
}
