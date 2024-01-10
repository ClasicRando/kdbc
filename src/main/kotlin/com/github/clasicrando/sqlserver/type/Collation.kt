package com.github.clasicrando.sqlserver.type

import io.ktor.utils.io.charsets.Charset

class Collation(private val info: UInt, private val sortId: UByte) {
    val lcid: UShort get() = (info and 0xFFFFu).toUShort()

    private fun lcidToEncoding(): Charset? {
        return when (lcid) {
            0x0401u.toUShort() -> windows1256
            0x0402u.toUShort() -> windows1251
            0x0403u.toUShort() -> windows1252
            // CP950
            0x0404u.toUShort(), 0x0c04u.toUShort(), 0x1404u.toUShort() -> big5
            0x0405u.toUShort() -> windows1250
            0x0406u.toUShort() -> windows1252
            0x0407u.toUShort() -> windows1252
            0x0408u.toUShort() -> windows1253
            0x0409u.toUShort() -> windows1252
            0x040au.toUShort() -> windows1252
            0x040bu.toUShort() -> windows1252
            0x040cu.toUShort() -> windows1252
            0x040du.toUShort() -> windows1255
            0x040eu.toUShort() -> windows1250
            0x040fu.toUShort() -> windows1252
            0x0410u.toUShort() -> windows1252
            // CP932
            0x0411u.toUShort() -> shiftJis
            0x0412u.toUShort() -> eucKr
            0x0413u.toUShort() -> windows1252
            0x0414u.toUShort() -> windows1252
            0x0415u.toUShort() -> windows1250
            0x0416u.toUShort() -> windows1252
            0x0417u.toUShort() -> windows1252
            0x0418u.toUShort() -> windows1250
            0x0419u.toUShort() -> windows1251
            0x041au.toUShort() -> windows1250
            0x041bu.toUShort() -> windows1250
            0x041cu.toUShort() -> windows1250
            0x041du.toUShort() -> windows1252
            0x041eu.toUShort() -> windows874
            0x041fu.toUShort() -> windows1254
            0x0420u.toUShort() -> windows1256
            0x0421u.toUShort() -> windows1252
            0x0422u.toUShort() -> windows1251
            0x0423u.toUShort() -> windows1251
            0x0424u.toUShort() -> windows1250
            0x0425u.toUShort() -> windows1257
            0x0426u.toUShort() -> windows1257
            0x0427u.toUShort() -> windows1257
            0x0428u.toUShort() -> windows1251
            0x0429u.toUShort() -> windows1256
            0x042au.toUShort() -> windows1258
            0x042bu.toUShort() -> windows1252
            0x042cu.toUShort() -> windows1254
            0x042du.toUShort() -> windows1252
            0x042eu.toUShort() -> windows1252
            0x042fu.toUShort() -> windows1251
            0x0432u.toUShort() -> windows1252
            0x0434u.toUShort() -> windows1252
            0x0435u.toUShort() -> windows1252
            0x0436u.toUShort() -> windows1252
            0x0437u.toUShort() -> windows1252
            0x0438u.toUShort() -> windows1252
            0x0439u.toUShort() -> utf16Le
            0x043au.toUShort() -> utf16Le
            0x043bu.toUShort() -> windows1252
            0x043eu.toUShort() -> windows1252
            0x043fu.toUShort() -> windows1251
            0x0440u.toUShort() -> windows1251
            0x0441u.toUShort() -> windows1252
            0x0442u.toUShort() -> windows1250
            0x0443u.toUShort() -> windows1254
            0x0444u.toUShort() -> windows1251
            0x0445u.toUShort() -> utf16Le
            0x0446u.toUShort() -> utf16Le
            0x0447u.toUShort() -> utf16Le
            0x0448u.toUShort() -> utf16Le
            0x0449u.toUShort() -> utf16Le
            0x044au.toUShort() -> utf16Le
            0x044bu.toUShort() -> utf16Le
            0x044cu.toUShort() -> utf16Le
            0x044du.toUShort() -> utf16Le
            0x044eu.toUShort() -> utf16Le
            0x044fu.toUShort() -> utf16Le
            0x0450u.toUShort() -> windows1251
            0x0451u.toUShort() -> utf16Le
            0x0452u.toUShort() -> windows1252
            0x0453u.toUShort() -> utf16Le
            0x0454u.toUShort() -> utf16Le
            0x0456u.toUShort() -> windows1252
            0x0457u.toUShort() -> utf16Le
            0x045au.toUShort() -> utf16Le
            0x045bu.toUShort() -> utf16Le
            0x045du.toUShort() -> windows1252
            0x045eu.toUShort() -> windows1252
            0x0461u.toUShort() -> utf16Le
            0x0462u.toUShort() -> windows1252
            0x0463u.toUShort() -> utf16Le
            0x0464u.toUShort() -> windows1252
            0x0465u.toUShort() -> utf16Le
            0x0468u.toUShort() -> windows1252
            0x046au.toUShort() -> windows1252
            0x046bu.toUShort() -> windows1252
            0x046cu.toUShort() -> windows1252
            0x046du.toUShort() -> windows1251
            0x046eu.toUShort() -> windows1252
            0x046fu.toUShort() -> windows1252
            0x0470u.toUShort() -> windows1252
            0x0478u.toUShort() -> windows1252
            0x047au.toUShort() -> windows1252
            0x047cu.toUShort() -> windows1252
            0x047eu.toUShort() -> windows1252
            0x0480u.toUShort() -> windows1256
            0x0481u.toUShort() -> utf16Le
            0x0482u.toUShort() -> windows1252
            0x0483u.toUShort() -> windows1252
            0x0484u.toUShort() -> windows1252
            0x0485u.toUShort() -> windows1251
            0x0486u.toUShort() -> windows1252
            0x0487u.toUShort() -> windows1252
            0x0488u.toUShort() -> windows1252
            0x048cu.toUShort() -> windows1256
            0x0801u.toUShort() -> windows1256
                // CP936
            0x0804u.toUShort(), 0x1004u.toUShort() -> gb18030
            0x0807u.toUShort() -> windows1252
            0x0809u.toUShort() -> windows1252
            0x080au.toUShort() -> windows1252
            0x080cu.toUShort() -> windows1252
            0x0810u.toUShort() -> windows1252
            0x0813u.toUShort() -> windows1252
            0x0814u.toUShort() -> windows1252
            0x0816u.toUShort() -> windows1252
            0x081au.toUShort() -> windows1250
            0x081du.toUShort() -> windows1252
            0x0827u.toUShort() -> windows1257
            0x082cu.toUShort() -> windows1251
            0x082eu.toUShort() -> windows1252
            0x083bu.toUShort() -> windows1252
            0x083cu.toUShort() -> windows1252
            0x083eu.toUShort() -> windows1252
            0x0843u.toUShort() -> windows1251
            0x0845u.toUShort() -> utf16Le
            0x0850u.toUShort() -> windows1251
            0x085du.toUShort() -> windows1252
            0x085fu.toUShort() -> windows1252
            0x086bu.toUShort() -> windows1252
            0x0c01u.toUShort() -> windows1256
            0x0c07u.toUShort() -> windows1252
            0x0c09u.toUShort() -> windows1252
            0x0c0au.toUShort() -> windows1252
            0x0c0cu.toUShort() -> windows1252
            0x0c1au.toUShort() -> windows1251
            0x0c3bu.toUShort() -> windows1252
            0x0c6bu.toUShort() -> windows1252
            0x1001u.toUShort() -> windows1256
            0x1007u.toUShort() -> windows1252
            0x1009u.toUShort() -> windows1252
            0x100au.toUShort() -> windows1252
            0x100cu.toUShort() -> windows1252
            0x101au.toUShort() -> windows1250
            0x103bu.toUShort() -> windows1252
            0x1401u.toUShort() -> windows1256
            0x1407u.toUShort() -> windows1252
            0x1409u.toUShort() -> windows1252
            0x140au.toUShort() -> windows1252
            0x140cu.toUShort() -> windows1252
            0x141au.toUShort() -> windows1250
            0x143bu.toUShort() -> windows1252
            0x1801u.toUShort() -> windows1256
            0x1809u.toUShort() -> windows1252
            0x180au.toUShort() -> windows1252
            0x180cu.toUShort() -> windows1252
            0x181au.toUShort() -> windows1250
            0x183bu.toUShort() -> windows1252
            0x1c01u.toUShort() -> windows1256
            0x1c09u.toUShort() -> windows1252
            0x1c0au.toUShort() -> windows1252
            0x1c1au.toUShort() -> windows1251
            0x1c3bu.toUShort() -> windows1252
            0x2001u.toUShort() -> windows1256
            0x2009u.toUShort() -> windows1252
            0x200au.toUShort() -> windows1252
            0x201au.toUShort() -> windows1251
            0x203bu.toUShort() -> windows1252
            0x2401u.toUShort() -> windows1256
            0x2409u.toUShort() -> windows1252
            0x240au.toUShort() -> windows1252
            0x243bu.toUShort() -> windows1252
            0x2801u.toUShort() -> windows1256
            0x2809u.toUShort() -> windows1252
            0x280au.toUShort() -> windows1252
            0x2c01u.toUShort() -> windows1256
            0x2c09u.toUShort() -> windows1252
            0x2c0au.toUShort() -> windows1252
            0x3001u.toUShort() -> windows1256
            0x3009u.toUShort() -> windows1252
            0x300au.toUShort() -> windows1252
            0x3401u.toUShort() -> windows1256
            0x3409u.toUShort() -> windows1252
            0x340au.toUShort() -> windows1252
            0x3801u.toUShort() -> windows1256
            0x380au.toUShort() -> windows1252
            0x3c01u.toUShort() -> windows1256
            0x3c0au.toUShort() -> windows1252
            0x4001u.toUShort() -> windows1256
            0x4009u.toUShort() -> windows1252
            0x400au.toUShort() -> windows1252
            0x4409u.toUShort() -> windows1252
            0x440au.toUShort() -> windows1252
            0x4809u.toUShort() -> windows1252
            0x480au.toUShort() -> windows1252
            0x4c0au.toUShort() -> windows1252
            0x500au.toUShort() -> windows1252
            0x540au.toUShort() -> windows1252
            else -> null
        }
    }

    private fun sortIdToEncoding(): Charset? {
        return when (sortId) {
            50u.toUByte() -> windows1252
            51u.toUByte() -> windows1252
            52u.toUByte() -> windows1252
            53u.toUByte() -> windows1252
            54u.toUByte() -> windows1252
            71u.toUByte() -> windows1252
            72u.toUByte() -> windows1252
            73u.toUByte() -> windows1252
            74u.toUByte() -> windows1252
            75u.toUByte() -> windows1252
            80u.toUByte() -> windows1250
            81u.toUByte() -> windows1250
            82u.toUByte() -> windows1250
            83u.toUByte() -> windows1250
            84u.toUByte() -> windows1250
            85u.toUByte() -> windows1250
            86u.toUByte() -> windows1250
            87u.toUByte() -> windows1250
            88u.toUByte() -> windows1250
            89u.toUByte() -> windows1250
            90u.toUByte() -> windows1250
            91u.toUByte() -> windows1250
            92u.toUByte() -> windows1250
            93u.toUByte() -> windows1250
            94u.toUByte() -> windows1250
            95u.toUByte() -> windows1250
            96u.toUByte() -> windows1250
            97u.toUByte() -> windows1250
            98u.toUByte() -> windows1250
            104u.toUByte() -> windows1251
            105u.toUByte() -> windows1251
            106u.toUByte() -> windows1251
            107u.toUByte() -> windows1251
            108u.toUByte() -> windows1251
            112u.toUByte() -> windows1253
            113u.toUByte() -> windows1253
            114u.toUByte() -> windows1253
            120u.toUByte() -> windows1253
            121u.toUByte() -> windows1253
            122u.toUByte() -> windows1253
            124u.toUByte() -> windows1253
            128u.toUByte() -> windows1254
            129u.toUByte() -> windows1254
            130u.toUByte() -> windows1254
            136u.toUByte() -> windows1255
            137u.toUByte() -> windows1255
            138u.toUByte() -> windows1255
            144u.toUByte() -> windows1256
            145u.toUByte() -> windows1256
            146u.toUByte() -> windows1256
            152u.toUByte() -> windows1257
            153u.toUByte() -> windows1257
            154u.toUByte() -> windows1257
            155u.toUByte() -> windows1257
            156u.toUByte() -> windows1257
            157u.toUByte() -> windows1257
            158u.toUByte() -> windows1257
            159u.toUByte() -> windows1257
            160u.toUByte() -> windows1257
            183u.toUByte() -> windows1252
            184u.toUByte() -> windows1252
            185u.toUByte() -> windows1252
            186u.toUByte() -> windows1252
            192u.toUByte(), 193u.toUByte(), 200u.toUByte() -> shiftJis
            194u.toUByte() -> eucKr
            195u.toUByte() -> eucKr
            196u.toUByte(), 197u.toUByte(), 202u.toUByte() -> big5
            198u.toUByte(), 199u.toUByte(), 203u.toUByte() -> gb18030
            201u.toUByte() -> big5
            204u.toUByte() -> windows874
            205u.toUByte() -> windows874
            206u.toUByte() -> windows874
            210u.toUByte() -> windows1252
            211u.toUByte() -> windows1252
            212u.toUByte() -> windows1252
            213u.toUByte() -> windows1252
            214u.toUByte() -> windows1252
            215u.toUByte() -> windows1252
            216u.toUByte() -> windows1252
            217u.toUByte() -> windows1252
            else -> null
        }
    }

    fun encoding(): Charset? {
        return if (sortId == zeroUByte) {
            lcidToEncoding()
        } else {
            sortIdToEncoding()
        }
    }

    override fun toString(): String {
        return encoding()?.name() ?: "None"
    }

    companion object {
        private val zeroUByte = 0u.toUByte()
        private val windows874 = Charset.forName("windows-874")
        private val windows1258 = Charset.forName("windows-1258")
        private val windows1257 = Charset.forName("windows-1257")
        private val windows1256 = Charset.forName("windows-1256")
        private val windows1255 = Charset.forName("windows-1255")
        private val windows1254 = Charset.forName("windows-1254")
        private val windows1253 = Charset.forName("windows-1253")
        private val windows1252 = Charset.forName("windows-1252")
        private val windows1251 = Charset.forName("windows-1251")
        private val windows1250 = Charset.forName("windows-1250")
        private val big5 = Charset.forName("big5")
        private val shiftJis = Charset.forName("shift-jis")
        private val eucKr = Charset.forName("euc-kr")
        private val utf16Le = Charset.forName("utf-16le")
        private val gb18030 = Charset.forName("GB18030")
    }
}
