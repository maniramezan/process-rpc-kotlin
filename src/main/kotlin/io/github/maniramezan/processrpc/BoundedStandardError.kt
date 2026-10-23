package io.github.maniramezan.processrpc

import java.io.InputStream

internal fun readStandardError(input: InputStream, maxLineBytes: Int, emit: (String) -> Unit) {
	val buffer = ByteArray(4_096)
	val line = ByteArray(maxLineBytes)
	var size = 0
	var truncated = false
	var carriageReturn = false
	fun publish() {
		emit(line.decodeToString(0, size) + if (truncated) " [truncated]" else "")
		size = 0
		truncated = false
	}
	input.use {
		while (true) {
			val count = it.read(buffer)
			if (count == -1) break
			for (index in 0 until count) {
				val byte = buffer[index]
				if (byte == 10.toByte() && carriageReturn) {
					carriageReturn = false
					continue
				}
				carriageReturn = byte == 13.toByte()
				if (byte == 10.toByte() || carriageReturn) publish()
				else if (size < line.size) line[size++] = byte
				else truncated = true
			}
		}
		if (size > 0 || truncated) publish()
	}
}
