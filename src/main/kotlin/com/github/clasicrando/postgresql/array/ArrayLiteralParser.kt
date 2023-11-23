package com.github.clasicrando.postgresql.array

object ArrayLiteralParser {
    private const val DELIMITER = ','

    fun parse(literal: String): Sequence<String?> = sequence {
        val charBuffer = literal.substring(1, literal.length - 1).toMutableList()
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

                    char == '"' -> inQuotes = true
                    char == '\\' -> inEscape = true
                    char == DELIMITER && !inQuotes -> {
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
