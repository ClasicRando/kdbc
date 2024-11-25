package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer

public sealed class MySqlValue(public val column: MySqlColumn) {
    public class Text(public val text: String, column: MySqlColumn) : MySqlValue(column) {
        override fun toString(): String {
            return "MySqlValue.Text(text=$text,column=$column)"
        }
    }

    public class Binary(public val bytes: ByteReadBuffer, column: MySqlColumn) :
        MySqlValue(column) {
        override fun toString(): String {
            return "MySqlValue.Binary(column=$column)"
        }
    }
}
