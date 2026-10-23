package io.github.maniramezan.processrpc

/** Machine-readable reasons for [ProcessRpcException] and [ProcessRpcState.Unavailable]. */
public enum class RpcFailureKind {
	/** The peer answered with a JSON-RPC error; inspect [ProcessRpcException.rpcCode]. */
	REMOTE_ERROR,
	/** A call, startup wait, or write exceeded its deadline. */
	TIMEOUT,
	/** The client has no active process, or the application stopped it. */
	NOT_RUNNING,
	/** A pending-call, write-count, or write-byte budget was exhausted. */
	OVERLOADED,
	/** An outgoing message exceeds the per-message byte limit. */
	MESSAGE_TOO_LARGE,
	/** Outgoing params were neither an object nor an array. */
	INVALID_PARAMS,
	/** The peer violated framing or JSON-RPC envelope rules. */
	PROTOCOL,
	/** Launching the process or communicating over its pipes failed. */
	TRANSPORT,
	/** A typed request could not be serialized. */
	ENCODING,
	/** A typed response could not be deserialized. */
	DECODING,
}
