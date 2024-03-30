package com.github.clasicrando.postgresql.type

/** Parse the [value] into separate points, yielding a [Sequence] of [String] values */
fun extractPoints(value: String) = sequence {
    if (value.isEmpty()) {
        return@sequence
    }
    var previousChar = value[0]
    val builder = StringBuilder()
    builder.append(previousChar)
    for (currentChar in value.asSequence().drop(1)) {
        if (previousChar == ')' && currentChar == ',') {
            yield(builder.toString())
            builder.clear()
            continue
        }
        builder.append(currentChar)
        previousChar = currentChar
    }
    if (builder.isNotEmpty()) {
        yield(builder.toString())
    }
}
