package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.postgresql.column.PgColumnDescription

data class PgPath(val isClosed: Boolean, val points: List<PgPoint>) {
    companion object {
        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1526
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgPath {
            val isClosed = readBuffer.readByte()
            val size = readBuffer.readInt()
            return PgPath(
                isClosed = isClosed == 1.toByte(),
                points = (1..size).map { PgPoint.fromBytes(readBuffer) },
            )
        }

        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1474
        internal fun fromStr(value: String, type: PgColumnDescription): PgPath {
            val isClosed = value[0] == '('
            val points = value.substring(1..(value.length - 2))
            return PgPath(
                isClosed = isClosed,
                points = points.split("),(").map { PgPoint.fromStr(it, type) }
            )
        }
    }
}
