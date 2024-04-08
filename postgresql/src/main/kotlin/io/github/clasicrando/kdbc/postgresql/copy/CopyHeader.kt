package io.github.clasicrando.kdbc.postgresql.copy

/**
 * Variations describing if the copy data includes a header. If the [Match] value is used and the
 * copy operation is a COPY FROM, the header needs to match the table definition exactly with name
 * and order of columns.
 */
enum class CopyHeader {
    True,
    False,
    Match,;

    override fun toString(): String {
        return when (this) {
            True -> TRUE
            False -> FALSE
            Match -> MATCH
        }
    }

    companion object {
        private const val TRUE = "true"
        private const val FALSE = "false"
        private const val MATCH = "MATCH"
    }
}
