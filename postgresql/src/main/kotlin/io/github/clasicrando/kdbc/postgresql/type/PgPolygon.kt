package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription

/**
 * PostGIS polygon type represented as a list of connected [points]. Polygons are very similar to a
 * closed [PgPath] but polygons are always closed, define a [boundBox] and are considered to
 * contain the area within the closed path.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-POLYGON)
 */
data class PgPolygon(val boundBox: PgBox, val points: List<PgPoint>) : PgGeometryType {
    constructor(points: List<PgPoint>): this(makeBoundBox(points), points)

    override val postGisLiteral: String
        get() = "(${points.joinToString(separator = ",") { it.postGisLiteral }})"

    companion object {
        fun makeBoundBox(points: List<PgPoint>): PgBox {
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

        /**
         * Decode [PgPolygon] from [readBuffer]. Reads:
         *
         * 1. [Int] - The number of points in the path
         * 2. The [List] of [PgPoint]s, providing the [readBuffer] to each call to
         * [PgPoint.fromBytes]. The [List] is the same size as the number of points read previously
         *
         * After the points are constructed a bounding box is generated based on those points to
         * find and max and min, x and y values and generate 2 [PgPoint] instances to make a
         * [PgBox].
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3510)
         *
         * @throws ColumnDecodeError if the number of points is zero
         */
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgPolygon {
            val size = readBuffer.readInt()
            val points = List(size) { PgPoint.fromBytes(readBuffer) }
            return PgPolygon(boundBox = makeBoundBox(points), points = points)
        }

        /**
         * Decode [PgPolygon] from [value]. Extracts 1 or more points from the [String] literal.
         * The expected format is '(({point}),...)'. The enclosing parenthesis are removed and each
         * point is mapped using [PgPoint.fromStr] to create the [List] of points.
         *
         * After the points are constructed a bounding box is generated based on those points to
         * find and max and min, x and y values and generate 2 [PgPoint] instances to make a
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3459)
         *
         * @throws ColumnDecodeError if any mapping of text to [PgPoint] fails or the number of
         * points is zero
         */
        internal fun fromStr(value: String, type: PgColumnDescription): PgPolygon {
            val pointsStr = value.substring(1, value.length - 1)
            val points = extractPoints(pointsStr)
                .map { PgPoint.fromStr(it, type) }
                .toList()
            return PgPolygon(boundBox = makeBoundBox(points), points = points)
        }
    }
}
