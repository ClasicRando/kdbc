package io.github.clasicrando.kdbc.mysql.load

public data class LoadLocalFileStatement(
    val targetTable: String,
    val duplicateHandling: DuplicateHandling = DuplicateHandling.IGNORE,
    val characterSet: String = "utf8mb4",
    val delimiter: Char = ',',
    val quoteChar: Char = '"',
    val newline: String = "\n",
    val skipLines: Int = 1,
    val columns: List<String> = emptyList(),
) {
    public enum class DuplicateHandling {
        REPLACE,
        IGNORE,
    }

    public fun toQuery(): String {
        return buildString {
            append("LOAD DATA LOCAL INFILE 'placeholder' ")
            append(duplicateHandling.name)
            append(" INTO TABLE ")
            append(targetTable.escape())
            append(" CHARACTER SET ")
            append(characterSet.escape())
            append(" FIELDS TERMINATED BY '")
            append(delimiter)
            append("' ENCLOSED BY '")
            append(quoteChar)
            append("' ESCAPED BY '' LINES TERMINATED BY '")
            append(newline)
            append("' STARTING BY '' IGNORE ")
            append(skipLines)
            append(" LINES")
            if (columns.isNotEmpty()) {
                columns.joinTo(this, separator = ",", prefix = " (", postfix = ")") { it.escape() }
            }
        }
    }

    private fun String.escape(): String {
        return "`" + this.replace("`", "``") + "`"
    }

    override fun toString(): String {
        return "LoadLocalFileStatement(targetTable='$targetTable', duplicateHandling=$duplicateHandling, characterSet='$characterSet', delimiter=$delimiter, quoteChar=$quoteChar, newline='$newline', skipLines=$skipLines, columns=$columns)"
    }
}
