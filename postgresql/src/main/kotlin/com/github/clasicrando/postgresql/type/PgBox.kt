package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.column.checkOrColumnDecodeError
import com.github.clasicrando.postgresql.column.PgColumnDescription

data class PgBox(val high: PgPoint, val low: PgPoint) {
    companion object {
        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L501
        internal fun fromBytes(readBuffer: ReadBuffer): PgBox {
            return PgBox(
                high = PgPoint.fromBytes(readBuffer),
                low = PgPoint.fromBytes(readBuffer),
            )
        }

        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L455
        internal fun fromStr(value: String, type: PgColumnDescription): PgBox {
            val points = value.split("),(")
            checkOrColumnDecodeError<PgBox>(points.size == 2, type)
            return PgBox(
                high = PgPoint.fromStr(points[0], type),
                low = PgPoint.fromStr(points[1], type),
            )
        }
    }
}
