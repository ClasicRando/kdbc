package com.github.kdbc.benchmarks.postgresql

import kotlinx.datetime.LocalDateTime

data class PostDataClass(
    val id: Int,
    val text: String,
    val creationDate: LocalDateTime,
    val lastChangeDate: LocalDateTime,
    val counter1: Int?,
    val counter2: Int?,
    val counter3: Int?,
    val counter4: Int?,
    val counter5: Int?,
    val counter6: Int?,
    val counter7: Int?,
    val counter8: Int?,
    val counter9: Int?,
)
