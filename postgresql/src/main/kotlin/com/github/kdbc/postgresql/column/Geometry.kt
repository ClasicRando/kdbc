package com.github.kdbc.postgresql.column

import com.github.kdbc.core.column.ColumnDecodeError
import com.github.kdbc.postgresql.type.PgBox
import com.github.kdbc.postgresql.type.PgCircle
import com.github.kdbc.postgresql.type.PgLine
import com.github.kdbc.postgresql.type.PgLineSegment
import com.github.kdbc.postgresql.type.PgPath
import com.github.kdbc.postgresql.type.PgPoint
import com.github.kdbc.postgresql.type.PgPolygon

/**
 * Implementation of a [PgTypeEncoder] for the [PgPoint] type. This maps to the `point` type in a
 * postgresql database. The encoder writes the x and y coordinates of the point as [Double] values.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1853)
 */
val pointTypeEncoder = PgTypeEncoder<PgPoint>(PgType.Point) { value, buffer ->
    buffer.writeDouble(value.x)
    buffer.writeDouble(value.y)
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgPoint] type. This maps to the `point` type in a
 * postgresql database. Calls the appropriate from method on [PgPoint].
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [PgPoint]
 */
val pointTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgPoint.fromBytes(value.bytes)
        is PgValue.Text -> PgPoint.fromStr(value.text, value.typeData)
    }
}

/**
 * Implementation of a [PgTypeEncoder] for the [PgLine] type. This maps to the `line` type in a
 * postgresql database. The encoder writes all 3 [Double] values of the line.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1038)
 */
val lineTypeEncoder = PgTypeEncoder<PgLine>(PgType.Line) { value, buffer ->
    buffer.writeDouble(value.a)
    buffer.writeDouble(value.b)
    buffer.writeDouble(value.c)
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgLine] type. This maps to the `line` type in a
 * postgresql database. Calls the appropriate from method on [PgLine].
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [PgLine]
 */
val lineTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgLine.fromBytes(value.bytes)
        is PgValue.Text -> PgLine.fromStr(value.text, value.typeData)
    }
}

/**
 * Implementation of a [PgTypeEncoder] for the [PgLineSegment] type. This maps to the `lseg` type
 * in a postgresql database. The encoder writes both [PgPoint]s to the argument buffer.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2092)
 */
val lineSegmentTypeEncoder = PgTypeEncoder<PgLineSegment>(PgType.LineSegment) { value, buffer ->
    pointTypeEncoder.encode(value.point1, buffer)
    pointTypeEncoder.encode(value.point2, buffer)
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgLineSegment] type. This maps to the `lseg` type
 * in a postgresql database. Calls the appropriate from method on [PgLineSegment].
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [PgLineSegment]
 */
val lineSegmentTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgLineSegment.fromBytes(value.bytes)
        is PgValue.Text -> PgLineSegment.fromStr(value.text, value.typeData)
    }
}

/**
 * Implementation of a [PgTypeEncoder] for the [PgBox] type. This maps to the `box` type in a
 * postgresql database. The encoder writes both [PgPoint]s to the argument buffer.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L466)
 */
val boxTypeEncoder = PgTypeEncoder<PgBox>(PgType.Box) { value, buffer ->
    pointTypeEncoder.encode(value.high, buffer)
    pointTypeEncoder.encode(value.low, buffer)
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgBox] type. This maps to the `box` type in a
 * postgresql database. Calls the appropriate from method on [PgBox].
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [PgBox]
 */
val boxTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgBox.fromBytes(value.bytes)
        is PgValue.Text -> PgBox.fromStr(value.text, value.typeData)
    }
}

/**
 * Implementation of a [PgTypeEncoder] for the [PgPath] type. This maps to the `path` type in a
 * postgresql database. The encoder writes:
 *
 * 1. [Byte] - A flag indicating if the path is closed
 * 2. [Int] - The number of points in the path
 * 3. dynamic Each point encoded using [pointTypeEncoder]
 *
 * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1488)
 */
val pathTypeEncoder = PgTypeEncoder<PgPath>(PgType.Path) { value, buffer ->
    buffer.writeByte(if (value.isClosed) 1 else 0)
    buffer.writeInt(value.points.size)
    for (point in value.points) {
        pointTypeEncoder.encode(point, buffer)
    }
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgPath] type. This maps to the `path` type in a
 * postgresql database. Calls the appropriate from method on [PgPath].
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [PgPath]
 */
val pathTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgPath.fromBytes(value.bytes)
        is PgValue.Text -> PgPath.fromStr(value.text, value.typeData)
    }
}

/**
 * Implementation of a [PgTypeEncoder] for the [PgPolygon] type. This maps to the `path` type in a
 * postgresql database. The encoder writes:
 *
 * 1. [Int] - The number of points in the path
 * 2. dynamic - Each point encoded using [pointTypeEncoder]
 *
 * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3475)
 */
val polygonTypeEncoder = PgTypeEncoder<PgPolygon>(PgType.Polygon) { value, buffer ->
    buffer.writeInt(value.points.size)
    for (point in value.points) {
        pointTypeEncoder.encode(point, buffer)
    }
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgPolygon] type. This maps to the `polygon` type in
 * a postgresql database. Calls the appropriate from method on [PgPolygon].
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [PgPolygon]
 */
val polygonTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgPolygon.fromBytes(value.bytes)
        is PgValue.Text -> PgPolygon.fromStr(value.text, value.typeData)
    }
}

/**
 * Implementation of a [PgTypeEncoder] for the [PgCircle] type. This maps to the `path` type in a
 * postgresql database. The encoder writes the [PgCircle.center] using [pointTypeEncoder], followed
 * by the [PgCircle.radius].
 *
 * [pg source code](https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4703)
 */
val circleTypeEncoder = PgTypeEncoder<PgCircle>(PgType.Circle) { value, buffer ->
    pointTypeEncoder.encode(value.center, buffer)
    buffer.writeDouble(value.radius)
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgCircle] type. This maps to the `circle` type in a
 * postgresql database. Calls the appropriate from method on [PgCircle].
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [PgCircle]
 */
val circleTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgCircle.fromBytes(value.bytes)
        is PgValue.Text -> PgCircle.fromStr(value.text, value.typeData)
    }
}
