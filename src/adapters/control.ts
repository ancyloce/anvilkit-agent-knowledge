// The owner's client of Control's DispatchService for the original-dispatch
// query of an expired external-effect lease (DD-02 §4, DD-09 §1). Only
// CONFIRMED_NOT_SENT or DENIED release the lease; every other state,
// answer or error leaves it unreleased.
import {
	DispatchServiceClient,
	DispatchState,
	GetDispatchRequest,
} from "@anvilkit/generated-clients/proto/anvilkit/control/v1/dispatch";
import { validateJson } from "@anvilkit/generated-clients/validation/rpc";
import { type ChannelCredentials, type ChannelOptions, Metadata } from "@grpc/grpc-js";
import type { DispatchQuery } from "../application/tasks.js";
import type { DispatchOutcome } from "../domain/task.js";

export const owner = "anvilkit-agent-knowledge";

export class ControlDispatchQuery implements DispatchQuery {
	private readonly client: DispatchServiceClient;

	/**
	 * The transport is the rotating mTLS credential of the workload identity
	 * with hostname verification against Control's server name, or, under
	 * the DEVELOPMENT_ONLY guard, the plaintext credential the caller built.
	 */
	constructor(
		address: string,
		private readonly timeoutMs: number,
		transport: ChannelCredentials,
		options: ChannelOptions = {},
	) {
		this.client = new DispatchServiceClient(address, transport, options);
	}

	outcome(tenantId: string, dispatchId: string): Promise<DispatchOutcome> {
		const req = GetDispatchRequest.fromPartial({ dispatchId, owner, tenantId });
		const v = validateJson("anvilkit.control.v1.GetDispatchRequest", JSON.stringify(GetDispatchRequest.toJSON(req)));
		if (!v.valid) return Promise.reject(new Error("GetDispatchRequest invalid"));
		return new Promise((resolve, reject) => {
			this.client.getDispatch(req, new Metadata(), { deadline: Date.now() + this.timeoutMs }, (err, resp) => {
				if (err) return reject(err);
				switch (resp.dispatch?.state) {
					case DispatchState.DISPATCH_STATE_CONFIRMED_NOT_SENT:
					case DispatchState.DISPATCH_STATE_DENIED:
						return resolve("not_sent");
					case DispatchState.DISPATCH_STATE_OBSERVED:
						return resolve("sent");
					default:
						return resolve("unknown");
				}
			});
		});
	}

	close(): void {
		this.client.close();
	}
}
