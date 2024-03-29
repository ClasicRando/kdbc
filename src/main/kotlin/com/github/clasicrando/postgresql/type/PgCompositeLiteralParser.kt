package com.github.clasicrando.postgresql.type

object PgCompositeLiteralParser {
    private const val DELIMITER = ','

    fun parse(literal: String): Sequence<String?> = sequence {
        val charBuffer = literal.substring(1, literal.length - 1).toMutableList()
        var isDone = false
        val builder = StringBuilder()
        var quoted: Boolean

        while (!isDone) {
            builder.clear()
            quoted = false
            var foundDelimiter = false
            var inQuotes = false
            var inEscape = false
            var previousChar = '\u0000'
            while (charBuffer.isNotEmpty()) {
                val char = charBuffer.removeFirst()
                when {
                    inEscape -> {
                        builder.append(char)
                        inEscape = false
                    }
                    char == '"' && inQuotes -> inQuotes = false
                    char == '"' -> {
                        inQuotes = true
                        quoted = true
                        if (previousChar == '"') {
                            builder.append(char)
                        }
                    }
                    char == '\\' && !inEscape -> inEscape = true
                    char == DELIMITER && !inQuotes -> {
                        foundDelimiter = true
                        break
                    }
                    else -> builder.append(char)
                }
                previousChar = char
            }
            isDone = !foundDelimiter
            yield(builder.takeIf { it.isNotEmpty() || quoted }?.toString())
        }
    }
}
