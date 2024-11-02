package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer

sealed class MySqlValue {
    data class Text(
        val text: String,
    ) : MySqlValue()

    data class Binary(
        val bytes: ByteReadBuffer,
    ) : MySqlValue()
}
