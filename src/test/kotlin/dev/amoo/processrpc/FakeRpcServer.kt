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
fun main() {
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
