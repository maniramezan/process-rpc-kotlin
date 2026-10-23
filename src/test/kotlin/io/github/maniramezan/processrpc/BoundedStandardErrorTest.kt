package io.github.maniramezan.processrpc

import java.io.ByteArrayInputStream
import kotlin.test.Test
import kotlin.test.assertEquals

class BoundedStandardErrorTest {
	@Test
	fun `unterminated stderr retains only a bounded prefix`() {
		val lines = mutableListOf<String>()
		readStandardError(ByteArrayInputStream("x".repeat(100_000).encodeToByteArray()), 8, lines::add)
		assertEquals(listOf("xxxxxxxx [truncated]"), lines)
	}

	@Test
	fun `byte limits preserve unicode below the limit and empty lines`() {
		val lines = mutableListOf<String>()
		readStandardError(ByteArrayInputStream("éé\n\r\nlast".encodeToByteArray()), 4, lines::add)
		assertEquals(listOf("éé", "", "last"), lines)
	}
}
