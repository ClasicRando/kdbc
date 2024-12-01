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

    /**
     * Encrypt the supplied [password] using the `SHA-1` algorithm and the [authPluginData] as part
     * of the digestion process.
     */
    fun encryptPasswordSha1(password: String, authPluginData: ByteArray): ByteArray {
        return encryptPassword(password, authPluginData, algorithm = "SHA-1")
    }

    /**
     * Encrypt the supplied [password] using the `SHA-256` algorithm and the [authPluginData] as
     * part of the digestion process.
     */
    fun encryptPasswordSha256(password: String, authPluginData: ByteArray): ByteArray {
        return encryptPassword(password, authPluginData, algorithm = "SHA-256")
    }

    /**
     * Encrypt the [password] by fetching the [MessageDigest] for the specific [algorithm], followed
     * by:
     * 1. [MessageDigest.digest] the password as bytes
     * 2. reset the [MessageDigest]
     * 3. [MessageDigest.digest] the result of the first digest
     * 4. reset the [MessageDigest]
     * 5. [MessageDigest.update] with the [authPluginData]
     * 6. [MessageDigest.update] with the result of the second digest
     * 7. [MessageDigest.digest] the current state
     * 8. Produce a [ByteArray] with the first digest XORed by the final digest result
     */
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

    /**
     * Encrypted the password as a null terminated string using the public key and [authPluginData]
     * supplied.
     */
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
