package com.github.clasicrando.postgresql.column

val charTypeEncoder = PgTypeEncoder<Char>(PgType.Char) { value, buffer ->
    buffer.writeByte(value.code.toByte())
}

val charTypeDecoder = PgTypeDecoder { value ->
    if (value.bytes.request(1)) {
        value.bytes.readByte().toInt().toChar()
    } else {
        0.toChar()
    }
}
