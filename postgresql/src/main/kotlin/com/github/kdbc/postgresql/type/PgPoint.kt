package com.github.kdbc.postgresql.type

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.column.ColumnDecodeError
import com.github.kdbc.core.column.checkOrColumnDecodeError
import com.github.kdbc.core.column.columnDecodeError
import com.github.kdbc.postgresql.column.PgColumnDescription

/**
 * PostGIS point type represented as a pair of coordinates in a two-dimensional space.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-GEOMETRIC-POINTS)
 */
data class PgPoint(val x: Double, val y: Double) : PgGeometryType {
    override val postGisLiteral: String get() = "($x,$y)"

    companion object {
        /**
         * Decode [PgPoint] from [readBuffer]. Extracts 2 [Double] values for the [x] and [y]
         * coordinates.
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1868)
         */
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgPoint {
            return PgPoint(x = readBuffer.readDouble(), y = readBuffer.readDouble())
        }

        /**
         * Decode [PgPoint] from [value]. Extracts 2 [Double] values for the [x] and [y]
         * coordinates from the [String] assuming the format is '({x},{y})'.
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1842)
         *
         * @throws ColumnDecodeError if 2 [Double] values cannot be extracted from the text
         */
        internal fun fromStr(value: String, type: PgColumnDescription): PgPoint {
            val coordinates = value.substring(1, value.length - 1).split(',')
            checkOrColumnDecodeError<PgPoint>(
                check = coordinates.size == 2,
                type = type,
            ) { "Cannot decode '$value' as point. Points must have 2 coordinates" }
            return PgPoint(
                x = coordinates[0].toDoubleOrNull()
                    ?: columnDecodeError<PgPoint>(
                        type = type,
                        reason = "Cannot convert first coordinate of '$value' into a Double",
                    ),
                y = coordinates[1].toDoubleOrNull()
                    ?: columnDecodeError<PgPoint>(
                        type = type,
                        reason = "Cannot convert first coordinate of '$value' into a Double",
                    ),
            )
        }
    }
}
