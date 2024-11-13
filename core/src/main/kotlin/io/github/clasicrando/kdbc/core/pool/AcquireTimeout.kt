package io.github.clasicrando.kdbc.core.pool

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

public class AcquireTimeout :
    KdbcException("Exceeded timeout of connection pool to acquire a connection")
