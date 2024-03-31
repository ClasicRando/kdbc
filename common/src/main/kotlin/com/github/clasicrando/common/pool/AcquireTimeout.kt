package com.github.clasicrando.common.pool

import com.github.clasicrando.common.exceptions.KdbcException

class AcquireTimeout : KdbcException("Exceeded timeout of connection pool to acquire a connection")
