package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription

/**
 * PostGIS circle type represented as the [center] of the circle and the [radius].
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-CIRCLE)
 */
data class PgCircle(val center: PgPoint, val radius: Double) : PgGeometryType {
    override val postGisLiteral: String get() = "<${center.postGisLiteral},$radius>"

    companion object {
        /**
         * Decode [PgLine] from [readBuffer]. Extracts a [PgPoint] using [PgPoint.fromBytes], then
         * reads a [Double] to get the [radius].
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4727)
         */
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgCircle {
            return PgCircle(
                center = PgPoint.fromBytes(readBuffer),
                radius = readBuffer.readDouble(),
            )
        }

        /**
         * Decode [PgLine] from [value]. The expected format is '<(x,y),r>' so the point component
         * is extracted and passed to [PgPoint.fromStr]
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4681)
         *
         * @throws ColumnDecodeError if the circle section cannot be found, the parsing to
         * [PgPoint] fails or the radius component is not a [Double]
         */
        internal fun fromStr(value: String, type: PgColumnDescription): PgCircle {
            val data = value.substring(1..(value.length - 2))
            val mid = value.indexOf("),")
            checkOrColumnDecodeError<PgCircle>(
                check = mid >= 0,
                type = type,
            ) { "Cannot find the index where the point component ends for circle = '$value'" }
            return PgCircle(
                center = PgPoint.fromStr(data.substring(0..<mid), type),
                radius = data.substring(mid + 1)
                    .toDoubleOrNull()
                    ?: columnDecodeError<PgCircle>(
                        type = type,
                        reason = "Cannot convert radius in '$value' to a Double value",
                    )
            )
        }
    }
}
