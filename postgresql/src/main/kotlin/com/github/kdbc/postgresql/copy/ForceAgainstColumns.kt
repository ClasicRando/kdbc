package com.github.kdbc.postgresql.copy

import com.github.kdbc.core.quoteIdentifier

/** All possible options for forcing columns to follow some behaviour */
interface ForceAgainstColumns {
    /** Only use the select [columns] specified in the [List] */
    class Select(val columns: List<String>): ForceAgainstColumns {
        /** return all the names separated by a comma and quoted as identifier */
        override fun toString(): String {
            return columns.joinToString(separator = ",") { it.quoteIdentifier() }
        }
    }

    /** Select all columns in the target table */
    object All: ForceAgainstColumns {
        private const val TO_STRING = "*"

        /** Always returns a "*" string */
        override fun toString(): String = TO_STRING
    }
}
