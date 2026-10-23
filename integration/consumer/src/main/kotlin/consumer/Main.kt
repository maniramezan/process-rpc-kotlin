package consumer

import io.github.maniramezan.processrpc.ContentLengthFraming
import io.github.maniramezan.processrpc.ProcessRpcClient
import io.github.maniramezan.processrpc.ProcessRpcState
import io.github.maniramezan.processrpc.callAs
import io.github.maniramezan.processrpc.notifyWith
import java.io.BufferedInputStream
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

@Serializable
data class Greeting(val name: String)

fun main(args: Array<String>) {
	if (args.firstOrNull() == "child") serve()
	else verifyConsumer()
}

private fun verifyConsumer() = runBlocking {
	val java = ProcessHandle.current().info().command().orElseThrow()
	val client = ProcessRpcClient(command = {
		listOf(java, "-cp", System.getProperty("java.class.path"), "consumer.MainKt", "child")
	})
	try {
		val stderr = async(start = CoroutineStart.UNDISPATCHED) { client.standardError.first() }
		client.start()
		check(client.callAs<Greeting, Greeting>("echo", Greeting("Ada")) == Greeting("Ada"))
		client.notifyWith("remember", Greeting("Grace"))
		check(client.callAs<Greeting>("recall") == Greeting("Grace"))
		check(withTimeout(5_000) { stderr.await() } == "consumer child ready")
		val pid = (client.state.value as ProcessRpcState.Ready).pid
		client.stop()
		check(client.state.value == ProcessRpcState.Stopped)
		check(!ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false))
		client.start()
		check(client.callAs<Greeting, Greeting>("echo", Greeting("restart")) == Greeting("restart"))
		client.stop()
		println("Consumer smoke passed on JVM ${Runtime.version().feature()}")
	} finally {
		client.close()
	}
}

private fun serve() {
	System.err.println("consumer child ready")
	val input = BufferedInputStream(System.`in`)
	var remembered: JsonElement = JsonNull
	while (true) {
		val bytes = ContentLengthFraming.readMessage(input) ?: return
		val request = Json.parseToJsonElement(bytes.decodeToString()).jsonObject
		val params = request["params"] ?: JsonNull
		val result = when (request["method"]?.jsonPrimitive?.content) {
			"remember" -> { remembered = params; continue }
			"recall" -> remembered
			else -> params
		}
		val response = buildJsonObject {
			put("jsonrpc", "2.0")
			put("id", request.getValue("id"))
			put("result", result)
		}
		ContentLengthFraming.writeMessage(response.toString().encodeToByteArray(), System.out)
	}
}
