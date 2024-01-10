package com.github.clasicrando.sqlserver.message

enum class FeatureLevel(val inner: UInt) {
    SqlServerV7(0x70000000u),
    SqlServer2000(0x71000000u),
    SqlServer2000Spi(0x71000001u),
    SqlServer2005(0x72090002u),
    SqlServer2008(0x730A0003u),
    SqlServer2008R2(0x730B0003u),
    SqlServerN(0x74000004u),
    ;

    fun doneRowCountBytes(): UByte {
        return if (inner >= SqlServer2005.inner) {
            8u
        } else {
            4u
        }
    }

    companion object {
        val default = FeatureLevel.SqlServerN
    }
}
