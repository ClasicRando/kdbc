package com.github.clasicrando.postgresql.array

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class TestArrayLiteralParser {
    @ParameterizedTest
    @MethodSource("validArrayStringLiterals")
    fun `parse should succeed for valid array string literal`(pair: Pair<String, List<String?>>) {
        val (literal, output) = pair
        val result = ArrayLiteralParser.parse(literal).toList()

        Assertions.assertIterableEquals(output, result)
    }

    companion object {
        @JvmStatic
        fun validArrayStringLiterals(): Stream<Pair<String, List<String?>>> {
            return Stream.of(
                "{1,2,3,4}" to listOf("1", "2", "3", "4"),
                "{test,1,also a test}" to listOf("test", "1", "also a test"),
                "{\"2023-01-01 02:22:26-01\"}" to listOf("2023-01-01 02:22:26-01"),
                "{\"(test,1,also a test)\",\"(test,1,also a test)\"}" to listOf(
                    "(test,1,also a test)",
                    "(test,1,also a test)"
                ),
                "{}" to listOf(),
                "{test,NULL,also a test}" to listOf("test", null, "also a test"),
            )
        }
    }
}
