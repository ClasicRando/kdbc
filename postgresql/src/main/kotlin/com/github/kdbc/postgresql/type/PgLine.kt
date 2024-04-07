package com.github.kdbc.postgresql.type

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.column.ColumnDecodeError
import com.github.kdbc.core.column.checkOrColumnDecodeError
import com.github.kdbc.core.column.columnDecodeError
import com.github.kdbc.postgresql.column.PgColumnDescription

/**
 * PostGIS line type represented as a linear equation:
 *
 * [a]x + [b]y + [c] = 0
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-LINE)
 */
data class PgLine(val a: Double, val b: Double, val c: Double) : PgGeometryType {
    override val postGisLiteral: String get() = "{$a,$b,$c}"

    companion object {
        /**
         * Decode [PgLine] from [readBuffer]. Extracts 3 [Double] values for the [a], [b] and [c]
         * values.
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1061)
         */
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgLine {
            return PgLine(
                a = readBuffer.readDouble(),
                b = readBuffer.readDouble(),
                c = readBuffer.readDouble(),
            )
        }

        /**
         * Decode [PgLine] from [value]. Extracts 3 [Double] values for the [a], [b] and [c] from
         * the [String] assuming the format is '({a},{b},{c})'.
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1023)
         *
         * @throws ColumnDecodeError if 3 [Double] values cannot be extracted from the text
         */
        internal fun fromStr(value: String, type: PgColumnDescription): PgLine {
            val coordinates = value.substring(1..(value.length - 2)).split(',')
            checkOrColumnDecodeError<PgPoint>(
                check = coordinates.size == 3,
                type = type,
            ) { "Cannot decode '$value' as line. Lines must have 3 values" }
            return PgLine(
                a = coordinates[0].toDoubleOrNull()
                    ?: columnDecodeError<PgLine>(
                        type = type,
                        reason = "Could not convert first coordinate in '$value' to Double",
                    ),
                b = coordinates[1].toDoubleOrNull()
                    ?: columnDecodeError<PgLine>(
                        type = type,
                        reason = "Could not convert second coordinate in '$value' to Double",
                    ),
                c = coordinates[2].toDoubleOrNull()
                    ?: columnDecodeError<PgLine>(
                        type = type,
                        reason = "Could not convert third coordinate in '$value' to Double",
                    ),
            )
        }
    }
}
