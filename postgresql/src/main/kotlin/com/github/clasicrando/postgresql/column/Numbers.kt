package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.readDouble
import com.github.clasicrando.common.buffer.readFloat
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.buffer.readLong
import com.github.clasicrando.common.buffer.readShort
import com.github.clasicrando.common.buffer.writeDouble
import com.github.clasicrando.common.buffer.writeFloat
import com.github.clasicrando.common.buffer.writeInt
import com.github.clasicrando.common.buffer.writeLong
import com.github.clasicrando.common.buffer.writeShort
import kotlinx.io.readDouble
import kotlinx.io.readFloat

val shortTypeEncoder = PgTypeEncoder<Short>(PgType.Int2) { value, buffer ->
    buffer.writeShort(value)
}

val shortTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readShort()
        is PgValue.Text -> value.text.toShort()
    }
}

val intTypeEncoder = PgTypeEncoder<Int>(PgType.Int4) { value, buffer ->
    buffer.writeInt(value)
}

val intTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readInt()
        is PgValue.Text -> value.text.toInt()
    }
}

val longTypeEncoder = PgTypeEncoder<Long>(PgType.Int8) { value, buffer ->
    buffer.writeLong(value)
}

val longTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readLong()
        is PgValue.Text -> value.text.toLong()
    }
}

val floatTypeEncoder = PgTypeEncoder<Float>(PgType.Float4) { value, buffer ->
    buffer.writeFloat(value)
}

val floatTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readFloat()
        is PgValue.Text -> value.text.toFloat()
    }
}

val doubleTypeEncoder = PgTypeEncoder<Double>(PgType.Float8) { value, buffer ->
    buffer.writeDouble(value)
}

val doubleTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readDouble()
        is PgValue.Text -> value.text.toDouble()
    }
}
