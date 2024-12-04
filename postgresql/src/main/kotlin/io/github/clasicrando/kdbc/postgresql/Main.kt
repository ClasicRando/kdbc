package io.github.clasicrando.kdbc.postgresql

import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.InternalAPI
import io.ktor.utils.io.readInt
import io.ktor.utils.io.writeByte
import kotlinx.coroutines.runBlocking
import kotlinx.io.InternalIoApi

@OptIn(InternalIoApi::class, InternalAPI::class)
public fun main(): Unit = runBlocking {
    val channel = ByteChannel()
    channel.readBuffer.buffer.writeByte(1)
    channel.writeByte(1)
    channel.writeByte(1)
    channel.writeByte(1)
    channel.flush()
    println(channel.readInt())
}
