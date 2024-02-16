package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readDouble
import com.github.clasicrando.postgresql.row.PgColumnDescription

data class PgCircle(val center: PgPoint, val radius: Double) {
    companion object {
        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4727
        internal fun fromBytes(readBuffer: ReadBuffer): PgCircle {
            return PgCircle(
                center = PgPoint.fromBytes(readBuffer),
                radius = readBuffer.readDouble(),
            )
        }

        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4681
        internal fun fromStr(value: String, type: PgColumnDescription): PgCircle {
            val data = value.substring(1..(value.length - 2))
            val mid = value.indexOf("),")
            return PgCircle(
                center = PgPoint.fromStr(data.substring(0..<mid), type),
                radius = data.substring(mid + 1).toDouble()
            )
        }
    }
}
