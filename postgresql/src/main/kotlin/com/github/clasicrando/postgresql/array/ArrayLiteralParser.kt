package com.github.clasicrando.postgresql.array

/** Parser of Postgresql array literal string */
@PublishedApi
internal object ArrayLiteralParser {
    /**
     * Parse the provided [literal] into a [Sequence] of [String] chunks that represent each value
     * in the array
     */
    fun parse(literal: String): Sequence<String?> = sequence {
        val charBuffer = literal.substring(1, literal.length - 1).toMutableList()
        if (charBuffer.isEmpty()) {
            return@sequence
        }

        var isDone = false
        val builder = StringBuilder()

        while (!isDone) {
            builder.clear()
            var foundDelimiter = false
            var inQuotes = false
            var inEscape = false

            while (charBuffer.isNotEmpty()) {
                val char = charBuffer.removeFirst()
                when {
                    inEscape -> {
                        builder.append(char)
                        inEscape = false
                    }

                    char == '"' -> inQuotes = !inQuotes
                    char == '\\' -> inEscape = true
                    char == ',' && !inQuotes -> {
                        foundDelimiter = true
                        break
                    }

                    else -> builder.append(char)
                }
            }
            isDone = !foundDelimiter
            yield(builder.toString().takeIf { it.isNotEmpty() && it != "NULL" })
        }
    }
}
