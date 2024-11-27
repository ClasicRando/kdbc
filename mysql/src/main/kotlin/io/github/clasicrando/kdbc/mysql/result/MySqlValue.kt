package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer

public sealed class MySqlValue(public val bytes: ByteReadBuffer, public val column: MySqlColumn) {
    public class Text(bytes: ByteReadBuffer, column: MySqlColumn) : MySqlValue(bytes, column) {
        public val text: String by lazy {
            val string = bytes.readText()
            bytes.reset()
            string
        }

        override fun toString(): String {
            return "MySqlValue.Text(text=$text,column=$column)"
        }
    }

    public class Binary(bytes: ByteReadBuffer, column: MySqlColumn) : MySqlValue(bytes, column) {
        override fun toString(): String {
            return "MySqlValue.Binary(column=$column)"
        }
    }
}
