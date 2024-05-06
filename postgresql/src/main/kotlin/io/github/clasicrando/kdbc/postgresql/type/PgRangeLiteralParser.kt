package io.github.clasicrando.kdbc.postgresql.type

object PgRangeLiteralParser {
    fun parse(literal: String): Sequence<String?> = sequence {
        val buffer = StringBuilder()
        var isDone = false
        var quoted = false
        var inQuotes = false
        var inEscape = false
        var prevChar = '\u0000'
        val iter = literal.iterator()

        while (!isDone) {
            buffer.clear()

            while (iter.hasNext()) {
                val currentChar = iter.nextChar()
                when {
                    inEscape -> {
                        buffer.append(currentChar)
                        inEscape = false
                    }
                    currentChar == '"' && inQuotes -> {
                        inQuotes = false
                    }
                    currentChar == '"' -> {
                        inQuotes = true
                        quoted = true

                        if (prevChar == '"') {
                            buffer.append('"')
                        }
                    }
                    currentChar == '\\' && !inEscape -> {
                        inEscape = true
                    }
                    currentChar == ',' && !inQuotes -> break
                    else -> buffer.append(currentChar)
                }
                prevChar = currentChar
            }
            isDone = iter.hasNext()
            yield(buffer.toString().takeIf { !(it.isEmpty() && !quoted) })
        }
    }
}
