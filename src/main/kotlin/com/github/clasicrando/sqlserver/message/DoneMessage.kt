package com.github.clasicrando.sqlserver.message

data class DoneMessage(val status: List<DoneStatus>, val cursorCommand: UShort, val rowsDone: Long)
