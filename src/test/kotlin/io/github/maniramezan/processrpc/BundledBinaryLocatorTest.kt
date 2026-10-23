package io.github.maniramezan.processrpc

import java.io.File
import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class BundledBinaryLocatorTest {
	private val root = Files.createTempDirectory("locator").toFile()

	@AfterTest
	fun tearDown() {
		root.deleteRecursively()
	}

	@Test
	fun `the override wins over PATH`() {
		val override = executable("override/tool")
		executable("bin/tool")

		val located = locator(environment = mapOf("TOOL_PATH" to override.path, "PATH" to dir("bin"))).locate()

		assertEquals(override.absolutePath, located)
	}

	@Test
	fun `a non-executable override is rejected`() {
		val file = File(root, "tool").apply { writeText("") }

		assertFailsWith<IllegalArgumentException> { locator(environment = mapOf("TOOL_PATH" to file.path)).locate() }
	}

	@Test
	fun `packaged Compose resources win over PATH`() {
		val packaged = executable("resources/tool")
		executable("bin/tool")

		val located = locator(
			environment = mapOf("PATH" to dir("bin")),
			properties = mapOf("compose.application.resources.dir" to dir("resources")),
		).locate()

		assertEquals(packaged.absolutePath, located)
	}

	@Test
	fun `empty PATH entries and directories are skipped`() {
		File(root, "first/tool").mkdirs()
		val found = executable("second/tool")
		val path = listOf("", dir("first"), dir("second")).joinToString(File.pathSeparator)

		assertEquals(found.absolutePath, locator(environment = mapOf("PATH" to path)).locate())
	}

	@Test
	fun `Windows resolves PATHEXT extensions`() {
		val found = executable("bin/tool.exe")

		val located = locator(
			environment = mapOf("PATH" to dir("bin"), "PATHEXT" to ".COM;.EXE"),
			properties = mapOf("os.name" to "Windows 11"),
		).locate()

		assertEquals(found.absolutePath, located)
	}

	@Test
	fun `a missing binary names the override variable`() {
		val error = assertFailsWith<IllegalStateException> { locator(environment = mapOf("PATH" to dir("bin"))).locate() }

		assertEquals(true, error.message?.contains("TOOL_PATH"))
	}

	private fun locator(environment: Map<String, String> = emptyMap(), properties: Map<String, String> = emptyMap()) =
		BundledBinaryLocator("tool", "TOOL_PATH", environment::get, properties::get)

	private fun dir(name: String): String = File(root, name).apply { mkdirs() }.path

	private fun executable(path: String): File = File(root, path).apply {
		parentFile.mkdirs()
		writeText("")
		setExecutable(true)
	}
}
