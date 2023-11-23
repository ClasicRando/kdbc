package com.github.clasicrando.common

val Byte.Companion.ZERO: Byte get() = 0

inline fun <T> Result<T>.mapError(block: (Throwable) -> Throwable): Result<T> {
    if (isSuccess) {
        return this
    }
    return Result.failure(block(this.exceptionOrNull()!!))
}
