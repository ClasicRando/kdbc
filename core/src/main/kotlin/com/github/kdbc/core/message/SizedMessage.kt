package com.github.kdbc.core.message

/**
 * Signifies a server message that has a known size. This can allow for more efficient message
 * packing into a single buffer when attempting to send multiple sequential messages without
 * waiting for each message to be pushed to the server.
 */
interface SizedMessage {
    /** Total size of the message in bytes */
    val size: Int
}
