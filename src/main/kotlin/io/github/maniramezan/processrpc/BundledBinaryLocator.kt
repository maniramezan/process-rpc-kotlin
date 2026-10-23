package io.github.maniramezan.processrpc

import java.io.File

public class BundledBinaryLocator(
	private val binaryName: String,
	private val overrideEnvironmentVariable: String,
) {
	public fun locate(): String {
		System.getenv(overrideEnvironmentVariable)?.takeIf(String::isNotBlank)?.let { return executable(it) }
		System.getProperty("compose.application.resources.dir")?.let { resources ->
			File(resources, binaryName).takeIf(File::canExecute)?.let { return it.absolutePath }
		}
		System.getenv("PATH").orEmpty().split(File.pathSeparator).forEach { directory ->
			File(directory, binaryName).takeIf(File::canExecute)?.let { return it.absolutePath }
		}
		error("Could not locate $binaryName; set $overrideEnvironmentVariable or install it on PATH")
	}

	private fun executable(path: String): String {
		val file = File(path)
		require(file.canExecute()) { "$overrideEnvironmentVariable points to a non-executable file: $path" }
		return file.absolutePath
	}
}
