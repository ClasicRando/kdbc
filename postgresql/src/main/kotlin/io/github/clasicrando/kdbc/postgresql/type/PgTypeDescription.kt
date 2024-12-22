package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.core.type.DbType
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlin.reflect.KType

public abstract class PgTypeDescription<T : Any>(
    /**
     * [PgType] that is referenced for this type description as the serialization input and
     * deserialization output
     */
    final override val dbType: PgType,
    /** Kotlin type of [T] that is recognized by this type description */
    final override val kType: KType,
) : DbType<T, PgValue, PgType> {
    /** Decode the bytes provided into the type [T] */
    public abstract fun decodeBytes(value: PgValue.Binary): T

    /** Decode the [String] provided into the type [T] */
    public abstract fun decodeText(value: PgValue.Text): T

    override fun isCompatible(dbType: PgType): Boolean {
        return dbType.oid == this.dbType.oid
    }

    override fun getActualType(value: T): PgType {
        return dbType
    }

    final override fun decode(value: PgValue): T {
        return try {
            when (value) {
                is PgValue.Binary -> decodeBytes(value)
                is PgValue.Text -> decodeText(value)
            }
        } catch (ex: ColumnDecodeError) {
            throw ex
        } catch (ex: Exception) {
            columnDecodeError(
                kType = kType,
                type = value.typeData,
                reason = "Failed to decode value for unexpected reason",
                cause = ex,
            )
        } finally {
            if (value is PgValue.Binary) {
                value.bytes.reset()
            }
        }
    }
}
