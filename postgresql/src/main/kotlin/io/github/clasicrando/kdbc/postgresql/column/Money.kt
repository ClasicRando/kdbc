package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.postgresql.type.PgMoney

/**
 * Implementation of [PgTypeEncoder] for [PgMoney]. This maps to the `money` type in a postgresql
 * database. Simply writes the integer value of the money as a [Long].
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/cash.c#L513)
 */
val moneyTypeEncoder = PgTypeEncoder<PgMoney>(PgType.Money) { value, buffer ->
    buffer.writeLong(value.integer)
}

/**
 * Implementation of [PgTypeDecoder] for [PgMoney]. This maps to the `money` type in a postgresql
 * database.
 *
 * ### Binary
 * Read a [Long] value and pass that to the constructor of [PgMoney].
 *
 * ### Text
 * Parse the [String] value sent using [PgMoney.fromString].
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/cash.c#L524)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/cash.c#L310)
 *
 * @throws ColumnDecodeError if the [String] values provided is not "t" or "f"
 */
val moneyTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgMoney(value.bytes.readLong())
        is PgValue.Text -> PgMoney.fromString(value.text)
    }
}
