package com.github.kdbc.core.pool

import com.github.kdbc.core.exceptions.KdbcException

class AcquireTimeout : KdbcException("Exceeded timeout of connection pool to acquire a connection")
