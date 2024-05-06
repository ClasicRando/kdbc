package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the
 * `text`/`name`/`bpchar`/`varchar`/`xml` types in a postgresql database.
 */
abstract class AbstractVarcharTypeDescription(pgType: PgType) : PgTypeDescription<String>(
    pgType = pgType,
    kType = typeOf<String>(),
) {
    /** Simply writes the [String] value to the buffer in UTF8 encoding */
    override fun encode(value: String, buffer: ByteWriteBuffer) {
        buffer.writeText(value)
    }

    /** Read the bytes as text using UFT8 encoding */
    override fun decodeBytes(value: PgValue.Binary): String {
        return value.bytes.readText()
    }

    /** Return the [PgValue.Text.text] value directly */
    override fun decodeText(value: PgValue.Text): String {
        return value.text
    }
}

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `text` types in
 * a postgresql database.
 */
object TextTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Text)

/**
 * Implementation of a [ArrayTypeDescription] for [String]. This maps to the `text[]` types in a
 * postgresql database.
 */
object TextArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.TextArray,
    innerType = TextTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `varchar` types
 * in a postgresql database.
 */
object VarcharTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Varchar)

/**
 * Implementation of a [ArrayTypeDescription] for [String]. This maps to the `varchar[]` types in a
 * postgresql database.
 */
object VarcharArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.VarcharArray,
    innerType = VarcharTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `xml` types in a
 * postgresql database.
 */
object XmlTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Xml)

/**
 * Implementation of a [ArrayTypeDescription] for [String]. This maps to the `xml[]` types in a
 * postgresql database.
 */
object XmlArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.XmlArray,
    innerType = XmlTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `name` types in
 * a postgresql database.
 */
object NameTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Name)

/**
 * Implementation of a [ArrayTypeDescription] for [String]. This maps to the `name[]` types in a
 * postgresql database.
 */
object NameArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.NameArray,
    innerType = TextTypeDescription,
)

/**
 * Implementation of a [PgTypeDescription] for the [String] type. This maps to the `bpchar` types
 * in a postgresql database.
 */
object BpcharTypeDescription : AbstractVarcharTypeDescription(pgType = PgType.Bpchar)

/**
 * Implementation of a [ArrayTypeDescription] for [String]. This maps to the `bpchar[]` types in a
 * postgresql database.
 */
object BpcharArrayTypeDescription : ArrayTypeDescription<String>(
    pgType = PgType.BpcharArray,
    innerType = TextTypeDescription,
)
