package com.github.clasicrando.postgresql.column

val charTypeEncoder = PgTypeEncoder<Byte>(PgType.Char) { value, buffer ->
    buffer.writeByte(value)
}

val charTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> if (value.bytes.remaining > 0) value.bytes.readByte() else 0
        is PgValue.Text -> {
            when (value.text.length) {
                4 -> {
                    val first = value.text[1].code shl 6
                    val second = value.text[2].code shl 3
                    val third = value.text[3].code
                    (first or second or third).toByte()
                }
                1 -> value.text[0].code.toByte()
                0 -> 0.toByte()
                else -> error("Received invalid \"char\" text, '${value.text}'")
            }
        }
    }
}
