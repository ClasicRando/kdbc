package io.github.clasicrando.kdbc.mysql

import io.github.clasicrando.kdbc.core.Database
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnection
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions

/** [Database] implementation for MySQL */
public object MySql : Database<MySqlConnection, MySqlConnectionOptions> {
    override suspend fun connection(connectOptions: MySqlConnectionOptions): MySqlConnection {
        return MySqlConnection.connect(connectOptions, pool = null)
    }
}
