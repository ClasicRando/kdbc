package com.github.clasicrando.sqlserver.message

sealed interface FeatureAcknowledge {
    sealed interface FederatedAuthAcknowledge : FeatureAcknowledge {
        class SecurityToken(val nonce: ByteArray?) : FederatedAuthAcknowledge
    }
}
