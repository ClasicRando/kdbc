package com.github.clasicrando.postgresql.notification

import com.github.clasicrando.postgresql.PgConnection
import kotlinx.coroutines.channels.Channel

internal interface PgNotificationConnection : PgConnection {
    val notificationsChannel: Channel<PgNotification>
}
