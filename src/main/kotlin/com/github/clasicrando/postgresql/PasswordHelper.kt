package com.github.clasicrando.postgresql

import java.security.MessageDigest
import java.security.NoSuchAlgorithmException


object PasswordHelper {
    private val Lookup = byteArrayOf(
        '0'.code.toByte(),
        '1'.code.toByte(),
        '2'.code.toByte(),
        '3'.code.toByte(),
        '4'.code.toByte(),
        '5'.code.toByte(),
        '6'.code.toByte(),
        '7'.code.toByte(),
        '8'.code.toByte(),
        '9'.code.toByte(),
        'a'.code.toByte(),
        'b'.code.toByte(),
        'c'.code.toByte(),
        'd'.code.toByte(),
        'e'.code.toByte(),
        'f'.code.toByte()
    )

    private fun bytesToHex(bytes: ByteArray, hex: ByteArray, offset: Int) {
        var pos = offset
        var i = 0
        while (i < 16) {
            val c = bytes[i].toInt() and 0xff
            var j = c shr 4
            hex[pos] = Lookup[j]
            pos += 1
            j = c and 0xf
            hex[pos] = Lookup[j]
            pos += 1
            i += 1
        }
    }

    @Throws(NoSuchAlgorithmException::class)
    fun encode(
        username: ByteArray,
        password: ByteArray,
        salt: ByteArray,
    ): ByteArray {
        val md = MessageDigest.getInstance("MD5")
        val hexDigest = ByteArray(35)
        md.update(password)
        md.update(username)
        bytesToHex(md.digest(), hexDigest, 0)
        md.update(hexDigest, 0, 32)
        md.update(salt)
        bytesToHex(md.digest(), hexDigest, 3)
        hexDigest[0] = 'm'.code.toByte()
        hexDigest[1] = 'd'.code.toByte()
        hexDigest[2] = '5'.code.toByte()
        return hexDigest
    }
}
