package com.github.kdbc.postgresql.type

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.column.ColumnDecodeError
import com.github.kdbc.core.column.checkOrColumnDecodeError
import com.github.kdbc.postgresql.column.PgColumnDescription

/**
 * PostGIS box type represented as the opposite corners of the box.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-GEOMETRIC-BOXES)
 */
data class PgBox(val high: PgPoint, val low: PgPoint) : PgGeometryType {
    override val postGisLiteral: String
        get() = "(${high.postGisLiteral},${low.postGisLiteral})"

    companion object {
        /**
         * Decode [PgBox] from [readBuffer]. Extracts 2 [PgPoint] values the 2 points that define
         * the bounds of the box.
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L501)
         */
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgBox {
            return PgBox(
                high = PgPoint.fromBytes(readBuffer),
                low = PgPoint.fromBytes(readBuffer),
            )
        }

        /**
         * Decode [PgLine] from [value]. Extracts 2 [PgPoint] values that define the bounds of the
         * box. The format of the string literal is '({point1}),({point2})' so we split by the
         * comma that separates the 2 points and pass each point to [PgPoint.fromStr].
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L455)
         *
         * @throws ColumnDecodeError if 2 points cannot be found or parsed from the components of
         * the box
         */
        internal fun fromStr(value: String, type: PgColumnDescription): PgBox {
            val points = extractPoints(value).map { PgPoint.fromStr(it, type) }
                .toList()
            checkOrColumnDecodeError<PgBox>(points.size == 2, type) {
                "Number of points found must be 2. Found ${points.size}"
            }
            return PgBox(high = points[0], low = points[1])
        }
    }
}
