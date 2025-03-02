package io.github.clasicrando.kdbc.postgresql

import io.github.clasicrando.kdbc.core.Database
import io.github.clasicrando.kdbc.postgresql.Postgres.connection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.listen.PgListener

/** [Database] implementation for Postgresql */
public object Postgres : Database<PgConnection, PgConnectOptions> {
    /**
     * Create a new [PgConnection] (or reuse an existing connection if any are available) using the
     * supplied [PgConnectOptions].
     */
    override suspend fun connection(connectOptions: PgConnectOptions): PgConnection {
        return PgConnection.connect(connectOptions = connectOptions, pool = null)
    }

    /** Create a new [PgListener] with a connection acquired from [connection] */
    public suspend fun listener(connectOptions: PgConnectOptions): PgListener {
        return PgListener(connection(connectOptions))
    }
}
