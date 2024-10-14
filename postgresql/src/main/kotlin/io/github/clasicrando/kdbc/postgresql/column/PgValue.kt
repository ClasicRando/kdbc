package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.postgresql.column.PgValue.Binary
import io.github.clasicrando.kdbc.postgresql.column.PgValue.Text

/**
 * Values sent from the database are either in a [Text] or [Binary] format (format codes are Text =
 * 0 and Binary = 1). In both cases, the values come from a [ByteReadBuffer] but the [Text]
 * variation immediately reads the buffer as a [String] and allows decoders to parse the [String].
 * For the [Binary] variant, the buffer is retained in the value, and it is accessible for
 * subsequent reads to extract the binary data needed for decoding.
 */
internal sealed class PgValue(val typeData: PgColumnDescription) {
    /**
     * [PgValue] variant for providing database sent [String] data representing the output/result
     * data in text format. The containing [text] will be parsed by decoders into the required
     * output type.
     */
    class Text @PublishedApi internal constructor(
        val text: String,
        typeData: PgColumnDescription,
    ) : PgValue(typeData) {
        constructor(bytes: ByteReadBuffer, typeData: PgColumnDescription)
                : this(bytes.readText(), typeData)

        override fun toString(): String {
            return "PgValue.Text(text=$text, typeData=$typeData)"
        }
    }

    /**
     * [PgValue] variant for providing database sent [Byte] data representing the output/result
     * data in binary format. The containing [bytes] will be read by decoders into the required
     * output type.
     */
    class Binary @PublishedApi internal constructor(
        val bytes: ByteReadBuffer,
        typeData: PgColumnDescription,
    ) : PgValue(typeData) {
        override fun toString(): String {
            return "PgValue.Binary(typeData=$typeData)"
        }
    }
}
