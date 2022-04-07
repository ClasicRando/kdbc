package com.clasicrando.kdbc

import java.sql.ResultSet

/** */
interface ResultSetParser<T> {

    /** */
    fun parse(rs: ResultSet): T

}
