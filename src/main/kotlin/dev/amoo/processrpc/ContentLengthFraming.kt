package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.io.EOFException
import java.io.OutputStream

public object ContentLengthFraming {
	public fun readMessage(input: BufferedInputStream): ByteArray? {
		val header = readHeader(input) ?: return null
		val length = header.lineSequence()
			.mapNotNull { line ->
				val parts = line.split(":", limit = 2)
				if (parts.size == 2 && parts[0].trim().equals("Content-Length", ignoreCase = true)) {
					parts[1].trim().toIntOrNull()
				} else null
			}
			.firstOrNull() ?: error("Missing Content-Length header")
		return input.readExactly(length)
	}

	public fun writeMessage(body: ByteArray, output: OutputStream) {
		output.write("Content-Length: ${body.size}\r\n\r\n".encodeToByteArray())
		output.write(body)
		output.flush()
	}

	private fun readHeader(input: BufferedInputStream): String? {
		val bytes = ArrayList<Byte>()
		while (true) {
			val byte = input.read()
			if (byte == -1) {
				if (bytes.isEmpty()) return null
				throw EOFException("Stream closed mid-header")
			}
			bytes += byte.toByte()
			if (bytes.size >= 4 && bytes.takeLast(4) == listOf(13, 10, 13, 10).map(Int::toByte)) break
		}
		return bytes.toByteArray().decodeToString()
	}

	private fun BufferedInputStream.readExactly(length: Int): ByteArray {
		require(length >= 0) { "Content-Length must not be negative" }
		val body = ByteArray(length)
		var offset = 0
		while (offset < length) {
			val count = read(body, offset, length - offset)
			if (count == -1) throw EOFException("Stream closed mid-body")
			offset += count
		}
		return body
	}
}
