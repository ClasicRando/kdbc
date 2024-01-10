package com.github.clasicrando.sqlserver.column

import com.github.clasicrando.sqlserver.type.TypeInfo

data class BaseMetadataColumn(val flags: List<ColumnFlag>, val type: TypeInfo)
