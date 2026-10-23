package io.github.maniramezan.processrpc

import java.io.File

/**
 * Finds the executable that a [ProcessRpcClient] launches, typically from its `command` lambda.
 *
 * [locate] checks, in order:
 * 1. the [overrideEnvironmentVariable], which must name an executable file when set;
 * 2. the `compose.application.resources.dir` system property, which Compose Desktop sets for packaged apps;
 * 3. each directory on `PATH`, skipping empty entries.
 *
 * On Windows, a [binaryName] without an extension also matches each `PATHEXT` extension, so `tool`
 * finds `tool.exe`.
 */
public class BundledBinaryLocator internal constructor(
	private val binaryName: String,
	private val overrideEnvironmentVariable: String,
	private val environment: (String) -> String?,
	private val property: (String) -> String?,
) {
	public constructor(binaryName: String, overrideEnvironmentVariable: String) :
		this(binaryName, overrideEnvironmentVariable, System::getenv, System::getProperty)

	/**
	 * Returns the absolute path of the first match.
	 *
	 * @throws IllegalArgumentException when the override variable is set but does not name an executable file.
	 * @throws IllegalStateException when no executable is found.
	 */
	public fun locate(): String {
		environment(overrideEnvironmentVariable)?.takeIf(String::isNotBlank)?.let { return executable(it) }
		property("compose.application.resources.dir")?.let { resources ->
			find(File(resources))?.let { return it }
		}
		environment("PATH").orEmpty().split(File.pathSeparator).filter(String::isNotBlank).forEach { directory ->
			find(File(directory))?.let { return it }
		}
		error("Could not locate $binaryName; set $overrideEnvironmentVariable or install it on PATH")
	}

	private fun find(directory: File): String? =
		candidateNames().map { File(directory, it) }.firstOrNull(::isExecutableFile)?.absolutePath

	// Windows resolves `tool` to `tool.exe` and friends through PATHEXT; other platforms use the name as is.
	private fun candidateNames(): List<String> {
		val windows = property("os.name").orEmpty().startsWith("Windows", ignoreCase = true)
		if (!windows || binaryName.contains('.')) return listOf(binaryName)
		val extensions = environment("PATHEXT")?.takeIf(String::isNotBlank) ?: ".COM;.EXE;.BAT;.CMD"
		return extensions.split(';').filter(String::isNotBlank).map { binaryName + it.lowercase() } + binaryName
	}

	private fun isExecutableFile(file: File): Boolean = file.isFile && file.canExecute()

	private fun executable(path: String): String {
		val file = File(path)
		require(isExecutableFile(file)) { "$overrideEnvironmentVariable points to a non-executable file: $path" }
		return file.absolutePath
	}
}
