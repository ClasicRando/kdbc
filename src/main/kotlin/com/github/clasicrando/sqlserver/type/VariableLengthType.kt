package com.github.clasicrando.sqlserver.type

enum class VariableLengthType(val inner: Short) {
    Guid(0x24),
    IntN(0x26),
    BitN(0x68),
    DecimalN(0x6A),
    NumericN(0x6C),
    FloatN(0x6D),
    Money(0x6E),
    DateTimeN(0x6F),
    BigVarBin(0xA5),
    BigVarChar(0xA7),
    BigBinary(0xAD),
    BigChar(0xAF),
    NVarchar(0xE7),
    NChar(0xEF),
    Xml(0xF1),

    Udt(0xF0),
    Text(0x23),
    Image(0x22),
    NText(0x63),

    SSVariant(0x62),
}
