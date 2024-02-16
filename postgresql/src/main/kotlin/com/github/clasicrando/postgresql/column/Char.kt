package com.github.clasicrando.postgresql.column

val charTypeEncoder = PgTypeEncoder<Char>(PgType.Char) { value, buffer ->
    buffer.writeByte(value.code.toByte())
}

val charTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> runCatching { value.bytes.readByte().toInt().toChar() }
            .getOrElse { 0.toChar() }
        is PgValue.Text -> {
            require(value.text.length <= 1) {
                "Cannot read a char from a multi character string"
            }
            value.text.getOrElse(0) { 0.toChar() }
        }
    }
}
