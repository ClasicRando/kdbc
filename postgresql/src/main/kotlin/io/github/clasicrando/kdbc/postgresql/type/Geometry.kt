package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.type.PgPolygon.Companion.makeBoundBox
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [PgPoint] type. This maps to the `point` type in
 * a postGIS enabled postgresql database.
 */
internal object PointTypeDescription : PgTypeDescription<PgPoint>(
    dbType = PgType.Point,
    kType = typeOf<PgPoint>(),
) {
    /**
     * Writes the x and y coordinates of the point as [Double] values.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1853)
     */
    override fun encode(
        value: PgPoint,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeDouble(value.x)
        buffer.writeDouble(value.y)
    }

    /**
     * Extracts 2 [Double] values for the [PgPoint.x] and [PgPoint.y] coordinates.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1868)
     */
    override fun decodeBytes(value: PgValue.Binary): PgPoint {
        return PgPoint(x = value.bytes.readDouble(), y = value.bytes.readDouble())
    }

    /**
     * Extracts 2 [Double] values for the [PgPoint.x] and [PgPoint.y] coordinates from the [String]
     * assuming the format is '({x},{y})'.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1842)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if 2 [Double] values cannot
     * be extracted from the text
     */
    override fun decodeText(value: PgValue.Text): PgPoint {
        val coordinates =
            value.text
                .substring(1, value.text.length - 1)
                .split(',')
        checkOrColumnDecodeError<PgPoint>(
            check = coordinates.size == 2,
            type = value.typeData,
        ) { "Cannot decode '$value' as point. Points must have 2 coordinates" }
        return PgPoint(
            x =
                coordinates[0].toDoubleOrNull()
                    ?: columnDecodeError<PgPoint>(
                        type = value.typeData,
                        reason = "Cannot convert first coordinate of '$value' into a Double",
                    ),
            y =
                coordinates[1].toDoubleOrNull()
                    ?: columnDecodeError<PgPoint>(
                        type = value.typeData,
                        reason = "Cannot convert first coordinate of '$value' into a Double",
                    ),
        )
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [PgLine] type. This maps to the `line` type in
 * a postGIS enabled postgresql database.
 */
internal object LineTypeDescription : PgTypeDescription<PgLine>(
    dbType = PgType.Line,
    kType = typeOf<PgLine>(),
) {
    /**
     * Writes all 3 [Double] values of the line.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1038)
     */
    override fun encode(
        value: PgLine,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeDouble(value.a)
        buffer.writeDouble(value.b)
        buffer.writeDouble(value.c)
    }

    /**
     * Extracts 3 [Double] values for the [PgLine.a], [PgLine.b] and [PgLine.c] values.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1061)
     */
    override fun decodeBytes(value: PgValue.Binary): PgLine {
        return PgLine(
            a = value.bytes.readDouble(),
            b = value.bytes.readDouble(),
            c = value.bytes.readDouble(),
        )
    }

    /**
     * Extracts 3 [Double] values for the [PgLine.a], [PgLine.b] and [PgLine.c] values from the
     * [String] assuming the format is '({a},{b},{c})'.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1023)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if 3 [Double] values cannot
     * be extracted from the text
     */
    override fun decodeText(value: PgValue.Text): PgLine {
        val coordinates =
            value.text
                .substring(1..(value.text.length - 2))
                .split(',')
        checkOrColumnDecodeError<PgPoint>(
            check = coordinates.size == 3,
            type = value.typeData,
        ) { "Cannot decode '$value' as line. Lines must have 3 values" }
        return PgLine(
            a =
                coordinates[0].toDoubleOrNull()
                    ?: columnDecodeError<PgLine>(
                        type = value.typeData,
                        reason = "Could not convert first coordinate in '$value' to Double",
                    ),
            b =
                coordinates[1].toDoubleOrNull()
                    ?: columnDecodeError<PgLine>(
                        type = value.typeData,
                        reason = "Could not convert second coordinate in '$value' to Double",
                    ),
            c =
                coordinates[2].toDoubleOrNull()
                    ?: columnDecodeError<PgLine>(
                        type = value.typeData,
                        reason = "Could not convert third coordinate in '$value' to Double",
                    ),
        )
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [PgLineSegment] type. This maps to the `lseg`
 * type in a postGIS enabled postgresql database.
 */
internal object LineSegmentTypeDescription : PgTypeDescription<PgLineSegment>(
    dbType = PgType.LineSegment,
    kType = typeOf<PgLineSegment>(),
) {
    /**
     * Writes both [PgPoint]s to the argument buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2092)
     */
    override fun encode(
        value: PgLineSegment,
        buffer: ByteWriteBuffer,
    ) {
        PointTypeDescription.encode(value.point1, buffer)
        PointTypeDescription.encode(value.point2, buffer)
    }

    /**
     * Extracts 2 [PgPoint] instances using [PointTypeDescription] to create a new [PgLineSegment].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2111)
     */
    override fun decodeBytes(value: PgValue.Binary): PgLineSegment {
        return PgLineSegment(
            point1 = PointTypeDescription.decodeBytes(value),
            point2 = PointTypeDescription.decodeBytes(value),
        )
    }

    /**
     * Extracts 2 [PgPoint] values the 2 points that define the bounds of the line segment. The
     * format of the string literal is '({point1},{point2})' so we split by the comma that
     * separates the 2 points and pass each point to [PointTypeDescription].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2081)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if 2 points cannot be found
     * or parsed from the components of the line segment
     */
    override fun decodeText(value: PgValue.Text): PgLineSegment {
        val pointsStr = value.text.substring(1, value.text.length - 1)
        val points =
            extractPoints(pointsStr)
                .map { PointTypeDescription.decodeText(PgValue.Text(it, value.typeData)) }
                .toList()
        checkOrColumnDecodeError<PgLineSegment>(points.size == 2, value.typeData) {
            "Number of points found must be 2. Found ${points.size}"
        }
        return PgLineSegment(point1 = points[0], point2 = points[1])
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [PgBox] type. This maps to the `box` type in a
 * postGIS enabled postgresql database.
 */
internal object BoxTypeDescription : PgTypeDescription<PgBox>(
    dbType = PgType.Box,
    kType = typeOf<PgBox>(),
) {
    /**
     * Writes both [PgPoint]s to the argument buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L466)
     */
    override fun encode(
        value: PgBox,
        buffer: ByteWriteBuffer,
    ) {
        PointTypeDescription.encode(value.high, buffer)
        PointTypeDescription.encode(value.low, buffer)
    }

    /**
     * Extracts 2 [PgPoint] values the 2 points that define the bounds of the box.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L501)
     */
    override fun decodeBytes(value: PgValue.Binary): PgBox {
        return PgBox(
            high = PointTypeDescription.decodeBytes(value),
            low = PointTypeDescription.decodeBytes(value),
        )
    }

    /**
     * Extracts 2 [PgPoint] values that define the bounds of the box. The format of the string
     * literal is '({point1}),({point2})' so we split by the comma that separates the 2 points and
     * pass each point to [PointTypeDescription.decodeText].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L455)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if 2 points cannot be found
     * or parsed from the components of the box
     */
    override fun decodeText(value: PgValue.Text): PgBox {
        val points =
            extractPoints(value.text)
                .map { PointTypeDescription.decodeText(PgValue.Text(it, value.typeData)) }
                .toList()
        checkOrColumnDecodeError<PgBox>(points.size == 2, value.typeData) {
            "Number of points found must be 2. Found ${points.size}"
        }
        return PgBox(high = points[0], low = points[1])
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [PgPath] type. This maps to the `path` type in a
 * postGIS enabled postgresql database.
 */
internal object PathTypeDescription : PgTypeDescription<PgPath>(
    dbType = PgType.Path,
    kType = typeOf<PgPath>(),
) {
    /**
     * Writes:
     *
     * 1. [Byte] - A flag indicating if the path is closed
     * 2. [Int] - The number of points in the path
     * 3. dynamic Each point encoded using [PointTypeDescription.encode]
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1488)
     */
    override fun encode(
        value: PgPath,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeByte(if (value.isClosed) 1 else 0)
        buffer.writeInt(value.points.size)
        for (point in value.points) {
            PointTypeDescription.encode(point, buffer)
        }
    }

    /**
     * Reads:
     *
     * 1. [Byte] - A flag indicating if the path is closed
     * 2. [Int] - The number of points in the path
     * 3. The [List] of [PgPoint]s, providing the [value] to each call to
     * [PointTypeDescription.decodeBytes]. The [List] is the same size as the number of points read
     * previously
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1526)
     */
    override fun decodeBytes(value: PgValue.Binary): PgPath {
        val isClosed = value.bytes.readByte()
        val size = value.bytes.readInt()
        return PgPath(
            isClosed = isClosed == 1.toByte(),
            points = List(size) { PointTypeDescription.decodeBytes(value) },
        )
    }

    /**
     * Extracts 1 or more points from the [String] literal. The first character is checked for '('
     * to check if the path is closed. The expected format is '(({point}),...)' for closed paths
     * and '[({point},...)]' for open paths. In both cases the enclosing characters are removed and
     * each point is mapped using [PointTypeDescription.decodeText] to create the [List] of points.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1474)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if any mapping of text to
     * [PgPoint] fails
     */
    override fun decodeText(value: PgValue.Text): PgPath {
        val isClosed = value.text[0] == '('
        val points = value.text.substring(1, value.text.length - 1)
        return PgPath(
            isClosed = isClosed,
            points =
                extractPoints(points)
                    .map { PointTypeDescription.decodeText(PgValue.Text(it, value.typeData)) }
                    .toList(),
        )
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [PgPolygon] type. This maps to the `polygon`
 * type in a postGIS enabled postgresql database.
 */
internal object PolygonTypeDescription : PgTypeDescription<PgPolygon>(
    dbType = PgType.Polygon,
    kType = typeOf<PgPolygon>(),
) {
    /**
     * Writes:
     *
     * 1. [Int] - The number of points in the path
     * 2. dynamic - Each point encoded using [PointTypeDescription.encode]
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3475)
     */
    override fun encode(
        value: PgPolygon,
        buffer: ByteWriteBuffer,
    ) {
        buffer.writeInt(value.points.size)
        for (point in value.points) {
            PointTypeDescription.encode(point, buffer)
        }
    }

    /**
     * Reads:
     *
     * 1. [Int] - The number of points in the path
     * 2. The [List] of [PgPoint]s, providing the [value] for each call to
     * [PointTypeDescription.decodeBytes]. The [List] is the same size as the number of points read
     * previously
     *
     * After the points are constructed a bounding box is generated based on those points to
     * find and max and min, x and y values and generate 2 [PgPoint] instances to make a
     * [PgBox].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3510)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the number of points is
     * zero
     */
    override fun decodeBytes(value: PgValue.Binary): PgPolygon {
        val size = value.bytes.readInt()
        val points = List(size) { PointTypeDescription.decodeBytes(value) }
        return PgPolygon(boundBox = makeBoundBox(points), points = points)
    }

    /**
     * Extracts 1 or more points from the [String] literal. The expected format is
     * '(({point}),...)'. The enclosing parenthesis are removed and each point is mapped using
     * [PointTypeDescription.decodeText] to create the [List] of points.
     *
     * After the points are constructed a bounding box is generated based on those points to
     * find and max and min, x and y values and generate 2 [PgPoint] instances to make a
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3459)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if any mapping of text to
     * [PgPoint] fails or the number of points is zero
     */
    override fun decodeText(value: PgValue.Text): PgPolygon {
        val pointsStr = value.text.substring(1, value.text.length - 1)
        val points =
            extractPoints(pointsStr)
                .map { PointTypeDescription.decodeText(PgValue.Text(it, value.typeData)) }
                .toList()
        return PgPolygon(boundBox = makeBoundBox(points), points = points)
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [PgCircle] type. This maps to the `circle` type
 * in a postGIS enabled postgresql database.
 */
internal object CircleTypeDescription : PgTypeDescription<PgCircle>(
    dbType = PgType.Circle,
    kType = typeOf<PgCircle>(),
) {
    /**
     * Writes the [PgCircle.center] using [PointTypeDescription.encode], followed by the
     * [PgCircle.radius].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4703)
     */
    override fun encode(
        value: PgCircle,
        buffer: ByteWriteBuffer,
    ) {
        PointTypeDescription.encode(value.center, buffer)
        buffer.writeDouble(value.radius)
    }

    /**
     * Extracts a [PgPoint] using [PointTypeDescription.decodeBytes], then reads a [Double] to get
     * the [PgCircle.radius].
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4727)
     */
    override fun decodeBytes(value: PgValue.Binary): PgCircle {
        return PgCircle(
            center = PointTypeDescription.decodeBytes(value),
            radius = value.bytes.readDouble(),
        )
    }

    /**
     * The expected format is '<(x,y),r>' so the point component is extracted and passed to
     * [PointTypeDescription.decodeText] while the radius is converted to a [Double] value.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4681)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the circle section
     * cannot be found, the parsing to
     * [PgPoint] fails or the radius component is not a [Double]
     */
    override fun decodeText(value: PgValue.Text): PgCircle {
        val data = value.text.substring(1..(value.text.length - 2))
        val mid = value.text.indexOf("),")
        checkOrColumnDecodeError<PgCircle>(
            check = mid >= 0,
            type = value.typeData,
        ) { "Cannot find the index where the point component ends for circle = '$value'" }
        val pointValue = PgValue.Text(data.substring(0..<mid), value.typeData)
        return PgCircle(
            center = PointTypeDescription.decodeText(pointValue),
            radius =
                data.substring(mid + 1)
                    .toDoubleOrNull()
                    ?: columnDecodeError<PgCircle>(
                        type = value.typeData,
                        reason = "Cannot convert radius in '$value' to a Double value",
                    ),
        )
    }
}
