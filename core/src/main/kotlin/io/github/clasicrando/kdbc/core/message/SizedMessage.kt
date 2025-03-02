package io.github.clasicrando.kdbc.core.message

/**
 * Signifies a server message that has a known size. This can allow for more efficient message
 * packing into a single buffer when attempting to send multiple sequential messages without waiting
 * for each message to be pushed to the server.
 */
public interface SizedMessage {
    /** Total size of the message in bytes */
    public val size: Int
}
