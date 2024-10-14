package io.github.clasicrando.kdbc.core.config

import io.github.oshai.kotlinlogging.Level
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

@Serializable
internal data class KdbcOptions(
    @Serializable(with = LevelKSerializer::class)
    var detailedLogging: Level,
)

internal object LevelKSerializer : KSerializer<Level> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor(
            serialName = "Level",
            kind = PrimitiveKind.STRING,
        )

    override fun deserialize(decoder: Decoder): Level = Level.valueOf(decoder.decodeString())

    override fun serialize(
        encoder: Encoder,
        value: Level,
    ) {
        encoder.encodeString(value.name)
    }
}
