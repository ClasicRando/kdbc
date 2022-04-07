package com.clasicrando.kdbc

import java.io.OutputStream

/** */
fun OutputStream.appendText(str: String) {
    this.write(str.toByteArray())
}
