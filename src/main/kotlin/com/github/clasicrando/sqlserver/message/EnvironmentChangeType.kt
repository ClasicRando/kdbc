package com.github.clasicrando.sqlserver.message

enum class EnvironmentChangeType(val inner: UByte) {
    Database(1u),
    Language(2u),
    CharacterSet(3u),
    PacketSize(4u),
    UnicodeDataSortingLID(5u),
    UnicodeDataSortingCFL(6u),
    SqlCollation(7u),
    BeginTransaction(8u),
    CommitTransaction(9u),
    RollbackTransaction(10u),
    EnlistDTCTransaction(11u),
    DefectTransaction(12u),
    Rtls(13u),
    PromoteTransaction(15u),
    TransactionManagerAddress(16u),
    TransactionEnded(17u),
    ResetConnection(18u),
    UserName(19u),
    Routing(20u),
}
