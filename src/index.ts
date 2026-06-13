export interface Env {
	TUNNEL_ROOM: DurableObjectNamespace;
}

// --- 1. Определение DTO и констант безопасности ---

// Максимальный размер входящего сообщения (например, 8 KB)
const MAX_MESSAGE_SIZE = 8 * 1024;
// Максимальная длина произвольного payload
const MAX_PAYLOAD_LENGTH = 4096;

// Допустимые типы сообщений
export type MessageType = 'system' | 'chat' | 'command' | 'data';

// Строгий интерфейс DTO
export interface TunnelMessageDTO {
	type: MessageType;
	payload: string; // Произвольная информация всегда передается как строка (можно использовать Base64 для бинарников)
	timestamp: number;
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
			this.broadcast(server, JSON.stringify(this.createSystemMessage('Tunnel established! Ready for commands.')));
		}

		return new Response(null, { status: 101, webSocket: client });
	}

	async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
		// 1. Защита от больших сообщений (до попытки парсинга)
		const msgSize = typeof message === 'string' ? message.length : message.byteLength;
		if (msgSize > MAX_MESSAGE_SIZE) {
			ws.send(JSON.stringify(this.createSystemMessage('Error: Message too large')));
			return; // Игнорируем или можно даже закрыть соединение: ws.close(1009, 'Message Too Big');
		}

		if (message instanceof ArrayBuffer) {
			ws.send(JSON.stringify(this.createSystemMessage('Error: Binary frames are not supported. Encode as Base64 in payload.')));
			return;
		}

		try {
			const parsed = JSON.parse(message);
			const validatedDTO = this.validateAndSanitize(parsed);

			if (!validatedDTO) {
				ws.send(JSON.stringify(this.createSystemMessage('Error: Invalid message format')));
				return;
			}

			const safeMessageStr = JSON.stringify(validatedDTO);
			this.broadcast(ws, safeMessageStr);

		} catch (e) {
			ws.send(JSON.stringify(this.createSystemMessage('Error: Malformed JSON')));
		}
	}

	async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean) {
		this.broadcast(ws, JSON.stringify(this.createSystemMessage('Peer disconnected.')));
		const sockets = this.state.getWebSockets();
		if (sockets.length === 0) {
			await this.state.storage.deleteAll();
		}
	}

	async alarm() {
		const timeoutMsg = JSON.stringify(this.createSystemMessage('Timeout. Room destroyed.'));
		const sockets = this.state.getWebSockets();
		for (const socket of sockets) {
			socket.send(timeoutMsg);
			socket.close(1011, 'Timeout');
		}
		await this.state.storage.deleteAll();
	}

	private broadcast(sender: WebSocket, safeMessage: string) {
		const sockets = this.state.getWebSockets();
		for (const socket of sockets) {
			if (socket !== sender) {
				socket.send(safeMessage);
			}
		}
	}

	private createSystemMessage(text: string): TunnelMessageDTO {
		return {
			type: 'system',
			payload: text,
			timestamp: Date.now(),
		};
	}

	private validateAndSanitize(data: any): TunnelMessageDTO | null {
		if (!data || typeof data !== 'object' || Array.isArray(data)) return null;

		const allowedTypes: MessageType[] = ['system', 'chat', 'command', 'data'];
		if (!allowedTypes.includes(data.type)) return null;

		if (typeof data.payload !== 'string') return null;

		let safePayload = data.payload;
		if (safePayload.length > MAX_PAYLOAD_LENGTH) {
			safePayload = safePayload.substring(0, MAX_PAYLOAD_LENGTH);
		}

		return {
			type: data.type,
			payload: safePayload,
			timestamp: typeof data.timestamp === 'number' ? data.timestamp : Date.now(),
		};
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