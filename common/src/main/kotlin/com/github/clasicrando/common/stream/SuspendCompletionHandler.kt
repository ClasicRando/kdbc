package com.github.clasicrando.common.stream

import java.nio.channels.CompletionHandler
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class SuspendCompletionHandler<T : Any>(
    private val continuation: Continuation<Result<T>>,
) : CompletionHandler<T?, Unit?> {
    override fun completed(result: T?, attachment: Unit?) {
        checkNotNull(result) { "Completion handler completion returned null" }
        continuation.resume(Result.success(result))
    }

    override fun failed(ex: Throwable?, attachment: Unit?) {
        continuation.resume(Result.failure(ex ?: UnknownCompletionHandler()))
    }
}
