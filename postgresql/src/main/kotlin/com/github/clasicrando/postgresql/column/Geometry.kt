package com.github.clasicrando.postgresql.column

import com.github.clasicrando.postgresql.type.PgBox
import com.github.clasicrando.postgresql.type.PgCircle
import com.github.clasicrando.postgresql.type.PgLine
import com.github.clasicrando.postgresql.type.PgLineSegment
import com.github.clasicrando.postgresql.type.PgPath
import com.github.clasicrando.postgresql.type.PgPoint
import com.github.clasicrando.postgresql.type.PgPolygon

// https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1853
val pointTypeEncoder = PgTypeEncoder<PgPoint>(PgType.Point) { value, buffer ->
    buffer.writeDouble(value.x)
    buffer.writeDouble(value.y)
}

val pointTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgPoint.fromBytes(value.bytes)
        is PgValue.Text -> PgPoint.fromStr(value.text, value.typeData)
    }
}

// https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1038
val lineTypeEncoder = PgTypeEncoder<PgLine>(PgType.Line) { value, buffer ->
    buffer.writeDouble(value.a)
    buffer.writeDouble(value.b)
    buffer.writeDouble(value.c)
}

val lineTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgLine.fromBytes(value.bytes)
        is PgValue.Text -> PgLine.fromStr(value.text, value.typeData)
    }
}

// https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L2092
val lineSegmentTypeEncoder = PgTypeEncoder<PgLineSegment>(PgType.LineSegment) { value, buffer ->
    pointTypeEncoder.encode(value.point1, buffer)
    pointTypeEncoder.encode(value.point2, buffer)
}

val lineSegmentTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgLineSegment.fromBytes(value.bytes)
        is PgValue.Text -> PgLineSegment.fromStr(value.text, value.typeData)
    }
}

// https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L466
val boxTypeEncoder = PgTypeEncoder<PgBox>(PgType.Box) { value, buffer ->
    pointTypeEncoder.encode(value.high, buffer)
    pointTypeEncoder.encode(value.low, buffer)
}

val boxTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgBox.fromBytes(value.bytes)
        is PgValue.Text -> PgBox.fromStr(value.text, value.typeData)
    }
}

// https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L1488
val pathTypeEncoder = PgTypeEncoder<PgPath>(PgType.Path) { value, buffer ->
    buffer.writeByte(if (value.isClosed) 1 else 0)
    buffer.writeInt(value.points.size)
    for (point in value.points) {
        pointTypeEncoder.encode(point, buffer)
    }
}

val pathTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgPath.fromBytes(value.bytes)
        is PgValue.Text -> PgPath.fromStr(value.text, value.typeData)
    }
}

// https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L3475
val polygonTypeEncoder = PgTypeEncoder<PgPolygon>(PgType.Path) { value, buffer ->
    buffer.writeInt(value.points.size)
    for (point in value.points) {
        pointTypeEncoder.encode(point, buffer)
    }
}

val polygonTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgPolygon.fromBytes(value.bytes)
        is PgValue.Text -> PgPolygon.fromStr(value.text, value.typeData)
    }
}

// https://github.com/postgres/postgres/blob/1fe66680c09b6cc1ed20236c84f0913a7b786bbc/src/backend/utils/adt/geo_ops.c#L4703
val circleTypeEncoder = PgTypeEncoder<PgCircle>(PgType.Path) { value, buffer ->
    pointTypeEncoder.encode(value.center, buffer)
    buffer.writeDouble(value.radius)
}

val circleTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> PgCircle.fromBytes(value.bytes)
        is PgValue.Text -> PgCircle.fromStr(value.text, value.typeData)
    }
}
