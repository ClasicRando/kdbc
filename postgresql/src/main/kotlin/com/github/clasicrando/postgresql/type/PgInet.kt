package com.github.clasicrando.postgresql.type

import java.net.InetAddress

data class PgInet(val address: String) {
    constructor(inetAddress: InetAddress): this(inetAddress.hostAddress)

    fun toInetAddress(): InetAddress {
        return InetAddress.getByName(address)
    }
}
