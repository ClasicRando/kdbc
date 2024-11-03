package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/**
 * [KdbcException] thrown when a stream has ended. This can happen when:
 * - the host terminated the connection
 * - the client has been closed
 * - the host sent EOF
 */
public open class EndOfStream(reason: String = "") :
    KdbcException("Stream has been closed or received EOF. $reason".trim())
