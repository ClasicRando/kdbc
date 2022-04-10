package com.clasicrando.kdbc

import java.sql.ResultSet

/** Interface for parsers created during KSP processing */
interface ResultSetParser<T> {

    /** Returns an instance of [T] after parsing the [rs] parameter */
    fun parse(rs: ResultSet): T

}
