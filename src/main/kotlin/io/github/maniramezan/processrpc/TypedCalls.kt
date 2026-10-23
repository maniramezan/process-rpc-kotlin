package io.github.maniramezan.processrpc

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement

/** The [Json] configuration the typed helpers use unless given another: lenient about unknown keys. */
public val processRpcJson: Json = Json { ignoreUnknownKeys = true }

/**
 * Calls [method] with raw JSON [params] and decodes its result as [R].
 *
 * @throws ProcessRpcException when the call fails or its result cannot be decoded as [R].
 */
public suspend inline fun <reified R> ProcessRpcClient.callAs(
	method: String,
	params: JsonElement = JsonObject(emptyMap()),
	timeoutMs: Long? = null,
	json: Json = processRpcJson,
): R {
	val result = call(method, params, timeoutMs)
	return try {
		json.decodeFromJsonElement<R>(result)
	} catch (error: IllegalArgumentException) {
		throw ProcessRpcException(null, "Could not decode the result of RPC call '$method': ${error.message}", error)
	}
}

/**
 * Calls [method] with [params] encoded from [P] and decodes its result as [R].
 *
 * @throws ProcessRpcException when the call fails or its result cannot be decoded as [R].
 */
public suspend inline fun <reified P, reified R> ProcessRpcClient.callAs(
	method: String,
	params: P,
	timeoutMs: Long? = null,
	json: Json = processRpcJson,
): R = callAs(method, json.encodeToJsonElement(params), timeoutMs, json)

/** Sends a notification whose params are encoded from [P]. */
public suspend inline fun <reified P> ProcessRpcClient.notifyWith(method: String, params: P, json: Json = processRpcJson) {
	notify(method, json.encodeToJsonElement(params))
}

/** Decodes this notification's params as [T]; throws [kotlinx.serialization.SerializationException] when they do not match. */
public inline fun <reified T> RpcNotification.paramsAs(json: Json = processRpcJson): T = json.decodeFromJsonElement(params)
