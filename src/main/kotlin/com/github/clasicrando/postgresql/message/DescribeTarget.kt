package com.github.clasicrando.postgresql.message

enum class DescribeTarget(val code: Byte) {
    PreparedStatement('S'.code.toByte()),
    Portal('P'.code.toByte()),
}
