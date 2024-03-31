package com.github.clasicrando.common.stream

import com.github.clasicrando.common.exceptions.KdbcException

/** [Exception] thrown when a stream has ended because the host terminated the connection */
class EndOfStream : KdbcException("Reached end of stream")
