package com.github.clasicrando.sqlserver.type

sealed interface TypeLength {
    data class Limited(val size: Short) : TypeLength
    data object  Max : TypeLength
}
