package com.github.clasicrando.common.result

/** Default implementation of a [DataRow] where the values are backed by an [Array] */
class ArrayDataRow(
    private val columnMapping: Map<String, Int>,
    private val values: Array<Any?>,
) : DataRow {
    override fun get(index: Int): Any? {
        require(index >= 0 && index < values.size) {
            "Specified index, $index is not in row. Row size = ${values.size}"
        }
        return values[index]
    }

    override fun indexFromColumn(column: String): Int {
        val result = columnMapping[column]
        if (result != null) {
            return result
        }
        val columns = columnMapping.entries.joinToString { "${it.key}->${it.value}" }
        error("Could not find column in mapping. Column = '$column', columns = $columns")
    }

    override fun toString(): String {
        return values.asSequence()
            .mapIndexed { i, value ->
                val name = columnMapping.entries.first { it.value == i }.key
                "$name=$value"
            }.joinToString(
                separator = ", ",
                prefix = "ArrayDataRow(",
                postfix = ")",
            )
    }
}