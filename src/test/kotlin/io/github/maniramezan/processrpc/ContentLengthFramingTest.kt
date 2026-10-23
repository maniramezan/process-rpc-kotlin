package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
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
}
