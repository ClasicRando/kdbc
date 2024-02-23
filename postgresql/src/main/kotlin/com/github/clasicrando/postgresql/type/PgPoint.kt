package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readDouble
import com.github.clasicrando.common.column.checkOrColumnDecodeError
import com.github.clasicrando.postgresql.column.PgColumnDescription

data class PgPoint(val x: Double, val y: Double) {
    companion object {
        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1868
        internal fun fromBytes(readBuffer: ReadBuffer): PgPoint {
            return PgPoint(x = readBuffer.readDouble(), y = readBuffer.readDouble())
        }

        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1842
        internal fun fromStr(value: String, type: PgColumnDescription): PgPoint {
            val coordinates = value.substring(1..(value.length - 2)).split(',')
            checkOrColumnDecodeError<PgPoint>(coordinates.size == 2, type)
            return PgPoint(
                x = coordinates[0].toDouble(),
                y = coordinates[1].toDouble(),
            )
        }
    }
}
