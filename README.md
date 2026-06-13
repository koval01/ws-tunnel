```python
"""example"""
import asyncio
import websockets
import requests
import json
import time
import sys

BASE_URL = "ws-tunnel.koval.workers.dev"

API_URL = f"https://{BASE_URL}"
WS_URL = f"wss://{BASE_URL}"

async def listen_for_messages(ws):
    try:
        async for message in ws:
            try:
                data = json.loads(message)
                msg_type = data.get("type")
                payload = data.get("payload", "")

                # Worker system messages
                if msg_type == "system":
                    print(f"\r[SYSTEM]: {payload}\n> ", end="", flush=True)

                # Standard chat messages
                elif msg_type == "chat":
                    print(f"\r[PEER]: {payload}\n> ", end="", flush=True)

                # Remote command execution requests
                elif msg_type == "command":
                    print(f"\r[SYSTEM]: Executing remote command: {payload}\n> ", end="", flush=True)
                    
                    try:
                        output = "cmd" # Replace with actual subprocess execution
                        if not output: 
                            output = "[Success: No output]"
                    except Exception as e:
                        output = f"[Error executing command]: {str(e)}"

                    # Pack execution result into data payload
                    response = json.dumps({
                        "type": "data", 
                        "payload": f"exec_res:{output}"
                    })
                    await ws.send(response)

                # Processing multiplexed data (pings, command outputs)
                elif msg_type == "data":
                    if payload.startswith("ping_req:"):
                        ts = payload.split(":", 1)[1]
                        pong = json.dumps({"type": "data", "payload": f"ping_res:{ts}"})
                        await ws.send(pong)
                        
                    elif payload.startswith("ping_res:"):
                        ts_str = payload.split(":", 1)[1]
                        rtt_ms = (time.time() - float(ts_str)) * 1000
                        print(f"\r[PING]: {rtt_ms:.2f} ms\n> ", end="", flush=True)
                        
                    elif payload.startswith("exec_res:"):
                        output = payload.split(":", 1)[1]
                        print(f"\r\n--- [ REMOTE OUTPUT ] ---\n{output}\n-------------------------\n> ", end="", flush=True)

            except json.JSONDecodeError:
                print(f"\r[PEER RAW]: {message}\n> ", end="", flush=True)
                
    except websockets.exceptions.ConnectionClosed:
        print("\r[SYSTEM]: Connection closed. Exiting...\n")
        sys.exit(0)

async def send_messages(ws):
    loop = asyncio.get_running_loop()
    await asyncio.sleep(0.5)
    
    print("\n💡 COMMANDS:")
    print("   /ping     - Check latency")
    print("   !command  - Execute terminal command on peer (e.g. !whoami, !ls)")
    print("   text      - Send normal message")
    print("> ", end="", flush=True)

    while True:
        msg = await loop.run_in_executor(None, input)
        
        if not msg.strip():
            print("\r> ", end="", flush=True)
            continue
            
        if msg.lower() in ['/exit', '/quit']:
            await ws.close()
            break
            
        elif msg.lower() == '/ping':
            # Pack ping request
            payload = json.dumps({
                "type": "data", 
                "payload": f"ping_req:{time.time()}"
            })
            await ws.send(payload)
            
        elif msg.startswith('!'):
            cmd_to_run = msg[1:].strip()
            if cmd_to_run:
                # Use strict "command" DTO type
                payload = json.dumps({
                    "type": "command", 
                    "payload": cmd_to_run
                })
                await ws.send(payload)
                print("⏳ Executing remote command...", end="", flush=True)
            else:
                print("\r> ", end="", flush=True)
        else:
            # Use strict "chat" DTO type
            payload = json.dumps({
                "type": "chat", 
                "payload": msg
            })
            await ws.send(payload)
            print("> ", end="", flush=True)

async def main():
    print("🚀 Anycast Command Tunnel (v3.0 - Strict DTO Shell)")
    print("1. Create Room (Host)")
    print("2. Join Room (Guest)")
    
    choice = input("\nSelect role (1 or 2): ").strip()
    
    if choice == '1':
        response = requests.post(f"{API_URL}/api/create")
        if response.status_code != 200:
            print("[ERROR]: Failed to create room.")
            return
        code = response.json().get("code")
        print(f"\n====================================")
        print(f"✅ ROOM CREATED! CODE: {code}")
        print(f"====================================\n")
        role = "host"
    elif choice == '2':
        code = input("Enter 8-digit code: ").strip()
        if len(code) != 8: return
        role = "guest"
    else: return

    ws_endpoint = f"{WS_URL}/ws/{code}?role={role}"
    
    try:
        async with websockets.connect(ws_endpoint) as ws:
            await asyncio.gather(listen_for_messages(ws), send_messages(ws))
    except Exception as e:
        print(f"[ERROR]: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
```