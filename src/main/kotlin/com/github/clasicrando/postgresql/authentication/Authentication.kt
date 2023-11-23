package com.github.clasicrando.postgresql.authentication

sealed interface Authentication {
    data object Ok : Authentication
//    data object KerberosV5 : Authentication
    data object CleartextPassword : Authentication
    class Md5Password(val salt: ByteArray) : Authentication
//    data object Gss : Authentication
//    data class GssContinue(val data: String) : Authentication
//    data object Sspi : Authentication
    data class Sasl(val authMechanisms: List<String>) : Authentication
    data class SaslContinue(val saslData: String) : Authentication
    data class SaslFinal(val saslData: String) : Authentication
}
