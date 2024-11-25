package io.github.clasicrando.kdbc.mysql

import io.github.clasicrando.kdbc.core.Database
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnection
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions
import io.github.clasicrando.kdbc.mysql.pool.MySqlPoolManager

public object MySql : Database<MySqlConnection, MySqlConnectionOptions> {
    override suspend fun connection(connectOptions: MySqlConnectionOptions): MySqlConnection {
        return MySqlPoolManager.acquireConnection(connectOptions)
    }
}
