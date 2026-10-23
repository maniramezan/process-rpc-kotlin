package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.EOFException
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class ContentLengthFramingTest {
	@Test
	fun `round trips payloads containing newlines`() {
		val body = "{\"message\":\"one\\ntwo\"}".encodeToByteArray()
		val output = ByteArrayOutputStream()
		ContentLengthFraming.writeMessage(body, output)

		val input = BufferedInputStream(ByteArrayInputStream(output.toByteArray()))
		assertContentEquals(body, ContentLengthFraming.readMessage(input))
		assertNull(ContentLengthFraming.readMessage(input))
	}

	@Test
	fun `header names are case-insensitive and other headers are ignored`() {
		val input = stream("content-type: application/json\r\ncontent-length: 2\r\n\r\n{}")

		assertContentEquals("{}".encodeToByteArray(), ContentLengthFraming.readMessage(input))
	}

	@Test
	fun `a header without Content-Length is rejected`() {
		assertFailsWith<IllegalStateException> { ContentLengthFraming.readMessage(stream("Content-Type: x\r\n\r\n{}")) }
	}

	@Test
	fun `a stream that ends mid-header fails`() {
		assertFailsWith<EOFException> { ContentLengthFraming.readMessage(stream("Content-Length: 2\r\n")) }
	}

	@Test
	fun `a stream that ends mid-body fails`() {
		assertFailsWith<EOFException> { ContentLengthFraming.readMessage(stream("Content-Length: 10\r\n\r\n{}")) }
	}

	private fun stream(text: String) = BufferedInputStream(ByteArrayInputStream(text.encodeToByteArray()))
}
