package io.github.clasicrando.kdbc.core.result

public sealed interface Either<L, R> {
    public data class Left<L, R>(val inner: L): Either<L, R>
    public data class Right<L, R>(val inner: R): Either<L, R>
}
