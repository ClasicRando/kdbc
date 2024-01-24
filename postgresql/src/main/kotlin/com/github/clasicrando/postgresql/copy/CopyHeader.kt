package com.github.clasicrando.postgresql.copy

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
