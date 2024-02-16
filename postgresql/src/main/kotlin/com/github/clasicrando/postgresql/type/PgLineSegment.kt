package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.column.checkOrColumnDecodeError
import com.github.clasicrando.postgresql.row.PgColumnDescription

data class PgLineSegment(val point1: PgPoint, val point2: PgPoint) {
    companion object {
        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2111
        internal fun fromBytes(readBuffer: ReadBuffer): PgLineSegment {
            return PgLineSegment(
                point1 = PgPoint.fromBytes(readBuffer),
                point2 = PgPoint.fromBytes(readBuffer),
            )
        }

        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2081
        internal fun fromStr(value: String, type: PgColumnDescription): PgLineSegment {
            val points = value.substring(1..(value.length - 2))
            val mid = value.indexOf("),(")
            checkOrColumnDecodeError<PgLineSegment>(mid >= 0, type)
            return PgLineSegment(
                point1 = PgPoint.fromStr(points.substring(0..mid), type),
                point2 = PgPoint.fromStr(points.substring((mid + 2)..<points.length), type),
            )
        }
    }
}
