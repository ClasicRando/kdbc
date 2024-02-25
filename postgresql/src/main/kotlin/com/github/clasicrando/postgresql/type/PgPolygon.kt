package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.postgresql.column.PgColumnDescription

data class PgPolygon(val boundBox: PgBox, val points: List<PgPoint>) {
    companion object {
        private fun makeBoundBox(points: List<PgPoint>): PgBox {
            require(points.isNotEmpty()) { "Cannot make bounding box for polygon with no points" }
            var x1 = points[0].x
            var x2 = points[0].x
            var y1 = points[0].y
            var y2 = points[0].y
            for (i in 1..<points.size) {
                if (points[i].x < x1) {
                    x1 = points[i].x
                }
                if (points[i].x > x2) {
                    x2 = points[i].x
                }
                if (points[i].y < y1) {
                    y1 = points[i].y
                }
                if (points[i].y < y2) {
                    y2 = points[i].y
                }
            }
            return PgBox(
                high = PgPoint(x2, y2),
                low = PgPoint(x1, y1),
            )
        }

        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3510
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgPolygon {
            val size = readBuffer.readInt()
            val points = (1..size).map { PgPoint.fromBytes(readBuffer) }
            return PgPolygon(boundBox = makeBoundBox(points), points = points)
        }

        // https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3459
        internal fun fromStr(value: String, type: PgColumnDescription): PgPolygon {
            val points = value.substring(1..(value.length - 2))
                .split("),(")
                .map { PgPoint.fromStr(it, type) }
            return PgPolygon(boundBox = makeBoundBox(points), points = points)
        }
    }
}
