package io.github.clasicrando.kdbc.core.stream

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

/** [Exception] thrown when a stream has ended because the host terminated the connection */
class EndOfStream : KdbcException("Reached end of stream")
