package io.github.clasicrando.kdbc.core.buffer

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

public class BufferOverflow(remaining: Int, required: Int) :
    KdbcException("Buffer remaining = $remaining, Required space = $required")
