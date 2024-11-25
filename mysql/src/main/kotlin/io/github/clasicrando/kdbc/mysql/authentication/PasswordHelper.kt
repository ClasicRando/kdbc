package io.github.clasicrando.kdbc.mysql.authentication

import java.security.KeyFactory
import java.security.MessageDigest
import java.security.PublicKey
import java.security.spec.X509EncodedKeySpec
import javax.crypto.Cipher
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

internal object PasswordHelper {
    private val publicKeyRegex =
        Regex("(-+BEGIN PUBLIC KEY-+\\r?\\n|\\n?-+END PUBLIC KEY-+\\r?\\n?)")

    fun encryptPasswordSha1(password: String, authPluginData: ByteArray): ByteArray {
        return encryptPassword(password, authPluginData, "SHA-1")
    }

    fun encryptPasswordSha256(password: String, authPluginData: ByteArray): ByteArray {
        return encryptPassword(password, authPluginData, "SHA-256")
    }

    private fun encryptPassword(
        password: String,
        authPluginData: ByteArray,
        algorithm: String,
    ): ByteArray {
        val messageDigest = MessageDigest.getInstance(algorithm)
        val passwordBytes = password.toByteArray()

        val stage1 = messageDigest.digest(passwordBytes)
        messageDigest.reset()

        val stage2 = messageDigest.digest(stage1)
        messageDigest.reset()

        messageDigest.update(authPluginData)
        messageDigest.update(stage2)

        val digest = messageDigest.digest()
        return ByteArray(stage1.size) { i ->
            (stage1[i].toInt() and 0xff xor (digest[i].toInt() and 0xff)).toByte()
        }
    }

    fun encryptWithPublicKey(
        publicKeyBytes: ByteArray,
        passwordCStringBytes: ByteArray,
        authPluginData: ByteArray,
    ): ByteArray {
        val publicKey = generatePublicKey(publicKeyBytes)
        val correctedSeed = authPluginData.copyOf(authPluginData.size - 1)
        val correctedSeedLength = correctedSeed.size

        val xOrArray =
            ByteArray(passwordCStringBytes.size) { i ->
                ((passwordCStringBytes[i].toInt() and 0xff) xor
                        (correctedSeed[i % correctedSeedLength].toInt() and 0xff))
                    .toByte()
            }
        val cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding")
        cipher.init(Cipher.ENCRYPT_MODE, publicKey)
        return cipher.doFinal(xOrArray)
    }

    @OptIn(ExperimentalEncodingApi::class)
    private fun generatePublicKey(publicKeyBytes: ByteArray): PublicKey {
        val publicKeyString = String(publicKeyBytes, Charsets.US_ASCII).replace(publicKeyRegex, "")
        val keyBytes = Base64.Mime.decode(publicKeyString)
        val spec = X509EncodedKeySpec(keyBytes)
        val keyFactory = KeyFactory.getInstance("RSA")
        return keyFactory.generatePublic(spec)
    }
}
