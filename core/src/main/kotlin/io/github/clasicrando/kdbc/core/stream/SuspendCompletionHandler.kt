package io.github.clasicrando.kdbc.core.stream

import java.nio.channels.CompletionHandler
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Implementation of a [CompletionHandler] for a generic type [T]. This resumes the [continuation]
 * normally with the returned value [T] when [CompletionHandler.completed] is called and resumes
 * the [continuation] with an exception when [CompletionHandler.failed] is called.
 */
class SuspendCompletionHandler<T : Any>(
    private val continuation: Continuation<T>,
) : CompletionHandler<T?, Unit?> {
    override fun completed(result: T?, attachment: Unit?) {
        checkNotNull(result) { "Completion handler completion returned null" }
        continuation.resume(result)
    }

    override fun failed(ex: Throwable?, attachment: Unit?) {
        continuation.resumeWithException(ex ?: UnknownCompletionHandler())
    }
}
