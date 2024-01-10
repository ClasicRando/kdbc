package com.github.clasicrando.sqlserver.type

sealed interface TypeInfo {
    enum class FixedLenType(val inner: Short) : TypeInfo {
        Null(0x1F),
        Int1(0x30),
        Bit(0x32),
        Int2(0x34),
        Int4(0x38),
        DateTime4(0x3A),
        Float4(0x3B),
        Money(0x3C),
        DateTime(0x3D),
        Float8(0x3E),
        Money4(0x7A),
        Int8(0x7F),
    }
    data class VariableSized(
        val type: VariableLengthType,
        val size: Long,
        val collation: Collation?,
    ) : TypeInfo
    data class VariableSizePrecision(
        val type: VariableLengthType,
        val size: Long,
        val precision: Short,
        val scale: Short,
    ) : TypeInfo
    data class Xml(val schema: XmlSchema?, val size: Long) : TypeInfo
}
