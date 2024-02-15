package com.github.clasicrando.postgresql.column

val charTypeEncoder = PgTypeEncoder<Char>(PgType.Char) { value, buffer ->
    buffer.writeByte(value.code.toByte())
}

val charTypeDecoder = PgTypeDecoder { value ->
    runCatching { value.bytes.readByte().toInt().toChar() }
        .getOrElse { 0.toChar() }
}
