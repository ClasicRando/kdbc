package com.github.clasicrando.common.stream

/** [Exception] thrown when a stream has ended because the host terminated the connection */
class EndOfStream : Exception("Reached end of stream")
