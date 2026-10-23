package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.io.EOFException
import java.io.IOException
import java.io.OutputStream

/** The peer violated Content-Length framing; the stream can no longer be read reliably. */
public class FramingException(message: String) : IOException(message)

public object ContentLengthFraming {
	/** The largest body [readMessage] accepts unless told otherwise: 64 MiB. */
	public const val DEFAULT_MAX_MESSAGE_BYTES: Int = 64 * 1024 * 1024

	/** The largest header block [readMessage] accepts before giving up on finding its end. */
	public const val MAX_HEADER_BYTES: Int = 8 * 1024

	public fun readMessage(input: BufferedInputStream, maxMessageBytes: Int = DEFAULT_MAX_MESSAGE_BYTES): ByteArray? {
		val header = readHeader(input) ?: return null
		val length = header.lineSequence()
			.mapNotNull { line ->
				val parts = line.split(":", limit = 2)
				if (parts.size == 2 && parts[0].trim().equals("Content-Length", ignoreCase = true)) {
					parts[1].trim().toIntOrNull() ?: throw FramingException("Invalid Content-Length: ${parts[1].trim()}")
				} else null
			}
			.firstOrNull() ?: throw FramingException("Missing Content-Length header")
		if (length < 0) throw FramingException("Content-Length must not be negative")
		if (length > maxMessageBytes) throw FramingException("Content-Length $length exceeds the $maxMessageBytes byte limit")
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
			if (bytes.size > MAX_HEADER_BYTES) throw FramingException("Header exceeds $MAX_HEADER_BYTES bytes without a blank line")
		}
		return bytes.toByteArray().decodeToString()
	}

	private fun BufferedInputStream.readExactly(length: Int): ByteArray {
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
