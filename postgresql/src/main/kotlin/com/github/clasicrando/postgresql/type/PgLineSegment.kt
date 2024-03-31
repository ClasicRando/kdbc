package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.column.checkOrColumnDecodeError
import com.github.clasicrando.postgresql.column.PgColumnDescription

/**
 * PostGIS line segment type represented as a pair of points that are endpoints of the segment.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-LSEG)
 */
data class PgLineSegment(val point1: PgPoint, val point2: PgPoint) : PgGeometryType {
    override val postGisLiteral: String
        get() = "(${point1.postGisLiteral},${point2.postGisLiteral})"

    companion object {
        /**
         * Decode [PgLineSegment] from [readBuffer]. Extracts 2 [PgPoint] instances using
         * [PgPoint.fromBytes] to create a new [PgLineSegment].
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2111)
         */
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgLineSegment {
            return PgLineSegment(
                point1 = PgPoint.fromBytes(readBuffer),
                point2 = PgPoint.fromBytes(readBuffer),
            )
        }

        /**
         * Decode [PgLineSegment] from [value]. Extracts 2 [PgPoint] values the 2 points that
         * define the bounds of the line segment. The format of the string literal is
         * '({point1},{point2})' so we split by the comma that separates the 2 points and pass
         * each point to [PgPoint.fromStr].
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2081)
         *
         * @throws ColumnDecodeError if 2 points cannot be found or parsed from the components of
         * the line segment
         */
        internal fun fromStr(value: String, type: PgColumnDescription): PgLineSegment {
            val pointsStr = value.substring(1, value.length - 1)
            val points = extractPoints(pointsStr).map { PgPoint.fromStr(it, type) }
                .toList()
            checkOrColumnDecodeError<PgLineSegment>(points.size == 2, type) {
                "Number of points found must be 2. Found ${points.size}"
            }
            return PgLineSegment(point1 = points[0], point2 = points[1])
        }
    }
}
