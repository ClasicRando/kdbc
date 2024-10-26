package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [PgMoney] type. This maps to the `money` type in
 * a postgresql database.
 */
internal object MoneyTypeDescription : PgTypeDescription<PgMoney>(
    dbType = PgType.Money,
    kType = typeOf<PgMoney>(),
) {
    /**
     * Writes the integer value of the money as a [Long].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/cash.c#L513)
     */
    override fun encode(
        value: PgMoney,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeLong(value.integer)
    }

    /**
     * Read a [Long] value and pass that to the constructor of [PgMoney].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/cash.c#L524)
     */
    override fun decodeBytes(value: PgValue.Binary): PgMoney {
        return PgMoney(value.bytes.readLong())
    }

    /**
     * Parse the [String] value sent using [PgMoney.fromString].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/cash.c#L310)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text value cannot be
     * parsed into a money value
     */
    override fun decodeText(value: PgValue.Text): PgMoney {
        return try {
            PgMoney.fromString(value.text)
        } catch (ex: Exception) {
            columnDecodeError<PgMoney>(
                type = value.typeData,
                reason = "Could not parse '${value.text}' into a money value. ${ex.message}",
            )
        }
    }
}
