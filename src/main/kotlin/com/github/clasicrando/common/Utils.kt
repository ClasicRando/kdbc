package com.github.clasicrando.common

import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.select

val Byte.Companion.ZERO: Byte get() = 0

inline fun <T> Result<T>.mapError(block: (Throwable) -> Throwable): Result<T> {
    if (isSuccess) {
        return this
    }
    return Result.failure(block(this.exceptionOrNull()!!))
}

sealed interface Loop {
    data object Continue : Loop
    data object Break : Loop
}

suspend inline fun selectLoop(crossinline block: SelectBuilder<Loop>.() -> Unit) {
    while (true) {
        val loop = select(builder = block)
        when (loop) {
            is Loop.Continue -> continue
            is Loop.Break -> break
        }
    }
}
