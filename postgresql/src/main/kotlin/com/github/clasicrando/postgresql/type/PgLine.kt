package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readDouble
import com.github.clasicrando.common.column.checkOrColumnDecodeError
import com.github.clasicrando.postgresql.column.PgColumnDescription

data class PgLine(val a: Double, val b: Double, val c: Double) {
    companion object {
        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1061
        internal fun fromBytes(readBuffer: ReadBuffer): PgLine {
            return PgLine(
                a = readBuffer.readDouble(),
                b = readBuffer.readDouble(),
                c = readBuffer.readDouble(),
            )
        }

        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1023
        internal fun fromStr(value: String, type: PgColumnDescription): PgLine {
            val coordinates = value.substring(1..(value.length - 2)).split(',')
            checkOrColumnDecodeError<PgLine>(coordinates.size == 3, type)
            return PgLine(
                a = coordinates[0].toDouble(),
                b = coordinates[1].toDouble(),
                c = coordinates[2].toDouble(),
            )
        }
    }
}
