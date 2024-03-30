package com.github.clasicrando.common.stream

import com.github.clasicrando.common.exceptions.KdbcException

/**
 * [Exception] thrown when a [CompletionHandler][java.nio.channels.CompletionHandler] fails but the
 * returned exception is null. This should never happen but since java types can be null, this will
 * cover that case.
 */
class UnknownCompletionHandler : KdbcException("No exception included in CompletionHandler")
