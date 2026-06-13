export interface Env {
	TUNNEL_ROOM: DurableObjectNamespace;
}

export class TunnelRoom {
	constructor(private state: DurableObjectState, private env: Env) { }

	async fetch(request: Request) {
		if (request.headers.get('Upgrade') !== 'websocket') {
			return new Response('Expected Upgrade: websocket', { status: 426 });
		}

		const url = new URL(request.url);
		const role = url.searchParams.get('role');

		const pair = new WebSocketPair();
		const [client, server] = Object.values(pair);

		this.state.acceptWebSocket(server, [role || 'unknown']);
		const sockets = this.state.getWebSockets();

		if (sockets.length > 2) {
			server.close(1011, 'Room is full');
			return new Response(null, { status: 101, webSocket: client });
		}

		if (sockets.length === 1) {
			await this.state.storage.setAlarm(Date.now() + 3 * 60 * 1000);
		} else if (sockets.length === 2) {
			await this.state.storage.deleteAlarm();
			sockets.forEach((ws) => ws.send(JSON.stringify({ type: 'system', msg: 'Tunnel established! Ready for commands.' })));
		}

		return new Response(null, { status: 101, webSocket: client });
	}

	async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
		const sockets = this.state.getWebSockets();
		for (const socket of sockets) {
			if (socket !== ws) {
				socket.send(message);
			}
		}
	}

	async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
		const sockets = this.state.getWebSockets();
		for (const socket of sockets) {
			socket.send(JSON.stringify({ type: 'system', msg: 'Peer disconnected.' }));
		}
		if (sockets.length === 0) {
			await this.state.storage.deleteAll();
		}
	}

	async alarm() {
		const sockets = this.state.getWebSockets();
		for (const socket of sockets) {
			socket.send(JSON.stringify({ type: 'system', msg: 'Timeout. Room destroyed.' }));
			socket.close(1011, 'Timeout');
		}
		await this.state.storage.deleteAll();
	}
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === '/api/create' && request.method === 'POST') {
			const code = Math.floor(10000000 + Math.random() * 90000000).toString();
			return new Response(JSON.stringify({ code }), {
				headers: { 'Content-Type': 'application/json' },
			});
		}

		if (url.pathname.startsWith('/ws/')) {
			const code = url.pathname.split('/')[2];
			if (!code || code.length !== 8) {
				return new Response(JSON.stringify({ error: 'Invalid code' }), { status: 400 });
			}

			const id = env.TUNNEL_ROOM.idFromName(code);
			const roomObject = env.TUNNEL_ROOM.get(id);

			return roomObject.fetch(request);
		}

		return new Response(JSON.stringify({ status: 'Anycast Tunnel API is running', version: '1.0' }), {
			headers: { 'Content-Type': 'application/json' },
		});
	},
};