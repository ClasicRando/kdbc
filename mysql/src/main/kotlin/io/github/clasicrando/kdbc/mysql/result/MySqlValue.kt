package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer

/**
 * Union of the 2 kinds of values sent from the server. This corresponds to the 2 protocols
 * available to the C/S protocol, Text and Binary.
 */
public sealed class MySqlValue(public val bytes: ByteReadBuffer, public val column: MySqlColumn) {
    /**
     * String encoded values sent from the server. Initially store the bytes supplied (UTF-8 encoded
     * text) and the [text] property is lazily evaluated as needed.
     */
    public class Text internal constructor(bytes: ByteArray, column: MySqlColumn) :
        MySqlValue(ByteReadBuffer(bytes), column) {
        public val text: String by lazy {
            val string = this.bytes.readText()
            this.bytes.reset()
            string
        }

        override fun toString(): String {
            return "MySqlValue.Text(text=$text,column=$column)"
        }
    }

    /**
     * Binary encoded values send from the server. Represented in a container that can be read
     * multiple times.
     */
    public class Binary internal constructor(bytes: ByteArray, column: MySqlColumn) :
        MySqlValue(ByteReadBuffer(bytes), column) {
        override fun toString(): String {
            return "MySqlValue.Binary(column=$column)"
        }
    }
}
