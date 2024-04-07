package com.github.kdbc.core.stream

import java.nio.channels.CompletionHandler
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * Implementation of a [CompletionHandler] for the [Void] type. This resumes the [continuation]
 * normally, always returning [Unit] when [CompletionHandler.completed] is called and resumes the
 * [continuation] with an exception when [CompletionHandler.failed] is called.
 */
class SuspendVoidCompletionHandler(
    private val continuation: Continuation<Unit>,
) : CompletionHandler<Void?, Unit?> {
    override fun completed(result: Void?, attachment: Unit?) {
        continuation.resume(Unit)
    }

    override fun failed(ex: Throwable?, attachment: Unit?) {
        continuation.resumeWithException(ex ?: UnknownCompletionHandler())
    }
}
