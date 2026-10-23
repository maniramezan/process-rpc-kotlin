package io.github.maniramezan.processrpc

import java.io.BufferedInputStream
import kotlin.system.exitProcess
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/** A scripted JSON-RPC child used by [ProcessRpcClientTest]; launched in its own JVM. */
fun main(args: Array<String>) {
	if (args.firstOrNull() == "sleep") {
		Thread.sleep(60_000)
		return
	}
	val input = BufferedInputStream(System.`in`)
	var remembered: JsonElement = JsonNull
	while (true) {
		val message = ContentLengthFraming.readMessage(input)?.let { Json.parseToJsonElement(it.decodeToString()).jsonObject } ?: return
		val id = message["id"]
		val params = message["params"] ?: JsonNull
		when (message["method"]?.jsonPrimitive?.content) {
			"echo" -> respond(id, params)
			"emit" -> {
				val count = params.jsonObject.getValue("count").jsonPrimitive.int
				repeat(count) { index -> send(buildJsonObject { put("jsonrpc", "2.0"); put("method", "tick"); put("params", buildJsonObject { put("index", index) }) }) }
				respond(id, buildJsonObject { put("sent", count) })
			}
			"askClient" -> {
				send(buildJsonObject { put("jsonrpc", "2.0"); put("id", "server-1"); put("method", "client.ping") })
				val reply = ContentLengthFraming.readMessage(input)?.let { Json.parseToJsonElement(it.decodeToString()).jsonObject } ?: return
				respond(id, reply["error"] ?: JsonNull)
			}
			"remember" -> remembered = params
			"recall" -> respond(id, remembered)
			"slow" -> {
				Thread.sleep(params.jsonObject.getValue("millis").jsonPrimitive.int.toLong())
				respond(id, buildJsonObject { put("done", true) })
			}
			"fail" -> send(buildJsonObject {
				put("jsonrpc", "2.0")
				put("id", id ?: JsonNull)
				put("error", buildJsonObject {
					put("code", -32000)
					put("message", "failed on purpose")
					put("data", buildJsonObject { put("detail", "scripted") })
				})
			})
			"garbage" -> {
				ContentLengthFraming.writeMessage("not json".encodeToByteArray(), System.out)
				respond(id, buildJsonObject { put("survived", true) })
			}
			"badFrame" -> {
				System.out.write("Bogus: 1\r\n\r\n".encodeToByteArray())
				System.out.flush()
				Thread.sleep(60_000)
			}
			"spawnSleeper" -> {
				val java = ProcessHandle.current().info().command().orElse("java")
				val sleeper = ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "io.github.maniramezan.processrpc.FakeRpcServerKt", "sleep").start()
				respond(id, buildJsonObject { put("pid", sleeper.pid()) })
			}
			"ignoreTerm" -> {
				sun.misc.Signal.handle(sun.misc.Signal("TERM")) { }
				respond(id, buildJsonObject { put("ignoring", true) })
				Thread.sleep(60_000)
			}
			"exit" -> exitProcess(0)
		}
	}
}

private fun respond(id: JsonElement?, result: JsonElement) {
	send(buildJsonObject { put("jsonrpc", "2.0"); put("id", id ?: JsonNull); put("result", result) })
}

private fun send(message: JsonObject) {
	ContentLengthFraming.writeMessage(Json.encodeToString(JsonObject.serializer(), message).encodeToByteArray(), System.out)
}

internal val JsonElement.intValue: Int get() = (this as JsonPrimitive).int
