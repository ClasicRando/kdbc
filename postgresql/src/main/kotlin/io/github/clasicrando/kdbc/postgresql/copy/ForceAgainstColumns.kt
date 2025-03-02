package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.core.quoteIdentifier

/** All possible options for forcing columns to follow some behaviour */
public interface ForceAgainstColumns {
    /** Only use the select [columns] specified in the [List] */
    public class Select(public val columns: List<String>) : ForceAgainstColumns {
        /** return all the names separated by a comma and quoted as identifier */
        override fun toString(): String {
            return columns.joinToString(separator = ",") { it.quoteIdentifier() }
        }
    }

    /** Select all columns in the target table */
    public object All : ForceAgainstColumns {
        /** Always returns a "*" string */
        override fun toString(): String = "*"
    }
}
