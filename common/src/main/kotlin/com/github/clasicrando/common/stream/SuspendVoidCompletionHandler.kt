package com.github.clasicrando.common.stream

import java.nio.channels.CompletionHandler
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume

class SuspendVoidCompletionHandler(
    private val continuation: Continuation<Result<Unit>>,
) : CompletionHandler<Void?, Unit?> {
    override fun completed(result: Void?, attachment: Unit?) {
        continuation.resume(Result.success(Unit))
    }

    override fun failed(ex: Throwable?, attachment: Unit?) {
        continuation.resume(Result.failure(ex ?: UnknownCompletionHandler()))
    }
}
