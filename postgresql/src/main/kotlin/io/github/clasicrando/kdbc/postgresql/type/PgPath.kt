package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription

/**
 * PostGIS path type represented as a list of connected [points]. Paths can either be closed or
 * open.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-geometric.html#DATATYPE-GEOMETRIC-PATHS)
 */
data class PgPath(val isClosed: Boolean, val points: List<PgPoint>) : PgGeometryType {
    override val postGisLiteral: String
        get() = buildString {
            append(if (isClosed) '(' else '[')
            for ((i, point) in points.withIndex()) {
                if (i > 0) {
                    append(',')
                }
                append(point.postGisLiteral)
            }
            append(if (isClosed) ')' else ']')
        }

    companion object {
        /**
         * Decode [PgPath] from [readBuffer]. Reads:
         *
         * 1. [Byte] - A flag indicating if the path is closed
         * 2. [Int] - The number of points in the path
         * 3. The [List] of [PgPoint]s, providing the [readBuffer] to each call to
         * [PgPoint.fromBytes]. The [List] is the same size as the number of points read previously
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1526)
         */
        internal fun fromBytes(readBuffer: ByteReadBuffer): PgPath {
            val isClosed = readBuffer.readByte()
            val size = readBuffer.readInt()
            return PgPath(
                isClosed = isClosed == 1.toByte(),
                points = List(size) { PgPoint.fromBytes(readBuffer) },
            )
        }

        /**
         * Decode [PgPath] from [value]. Extracts 1 or more points from the [String] literal.
         * The first character is checked for '(' to check if the path is closed. The expected
         * format is '(({point}),...)' for closed paths and '[({point},...)]' for open paths. In
         * both cases the enclosing characters are removed and each point is mapped using
         * [PgPoint.fromStr] to create the [List] of points.
         *
         * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1474)
         *
         * @throws ColumnDecodeError if any mapping of text to [PgPoint] fails
         */
        internal fun fromStr(value: String, type: PgColumnDescription): PgPath {
            val isClosed = value[0] == '('
            val points = value.substring(1, value.length - 1)
            return PgPath(
                isClosed = isClosed,
                points = extractPoints(points).map { PgPoint.fromStr(it, type) }.toList()
            )
        }
    }
}
