package io.github.clasicrando.kdbc.postgresql.notification

/**
 * Object containing details of a notification sent through the `LISTEN/NOTIFY` protocol within a
 * postgresql database. Contains the [channelName] that the notification was sent under and the
 * optional [payload] of the notification (defaults to an empty string when no payload specified).
 *
 * [listen](https://www.postgresql.org/docs/current/sql-listen.html)
 * [notify](https://www.postgresql.org/docs/current/sql-notify.html)
 */
data class PgNotification(val channelName: String, val payload: String)
