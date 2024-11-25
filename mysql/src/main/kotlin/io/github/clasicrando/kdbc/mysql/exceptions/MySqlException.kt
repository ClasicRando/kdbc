package io.github.clasicrando.kdbc.mysql.exceptions

import io.github.clasicrando.kdbc.core.exceptions.KdbcException

public class MySqlException(message: String, ex: Exception? = null) : KdbcException(message, ex)
