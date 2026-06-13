```python
"""example"""
import asyncio
import websockets
import requests
import json
import time
import sys
import subprocess

BASE_URL = "ws-tunnel.koval.workers.dev"

API_URL = f"https://{BASE_URL}"
WS_URL = f"wss://{BASE_URL}"

async def listen_for_messages(ws):
    try:
        async for message in ws:
            try:
                data = json.loads(message)
                msg_type = data.get("type")

                if msg_type == "system":
                    print(f"\r[SYSTEM]: {data.get('msg')}\n> ", end="", flush=True)

                elif msg_type == "ping_req":
                    pong = json.dumps({"type": "ping_res", "ts": data.get("ts")})
                    await ws.send(pong)
                elif msg_type == "ping_res":
                    rtt_ms = (time.time() - data.get("ts")) * 1000
                    print(f"\r[PING]: {rtt_ms:.2f} ms\n> ", end="", flush=True)

                elif msg_type == "cmd":
                    print(f"\r[PEER]: {data.get('payload')}\n> ", end="", flush=True)

                elif msg_type == "exec":
                    cmd = data.get("cmd")
                    print(f"\r[SYSTEM]: Executing remote command: {cmd}\n> ", end="", flush=True)
                    
                    try:
                        result = subprocess.run(
                            cmd, 
                            shell=True, 
                            capture_output=True, 
                            text=True, 
                            timeout=15
                        )
                        output = result.stdout.strip() if result.stdout else result.stderr.strip()
                        if not output: 
                            output = "[Success: No output]"
                    except Exception as e:
                        output = f"[Error executing command]: {str(e)}"

                    response = json.dumps({"type": "exec_res", "output": output})
                    await ws.send(response)

                elif msg_type == "exec_res":
                    print(f"\r\n--- [ REMOTE OUTPUT ] ---\n{data.get('output')}\n-------------------------\n> ", end="", flush=True)

            except json.JSONDecodeError:
                print(f"\r[PEER]: {message}\n> ", end="", flush=True)
                
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
            payload = json.dumps({"type": "ping_req", "ts": time.time()})
            await ws.send(payload)
            
        elif msg.startswith('!'):
            cmd_to_run = msg[1:].strip()
            if cmd_to_run:
                payload = json.dumps({"type": "exec", "cmd": cmd_to_run})
                await ws.send(payload)
                print("⏳ Executing remote command...", end="", flush=True)
            else:
                print("\r> ", end="", flush=True)
        else:
            payload = json.dumps({"type": "cmd", "payload": msg})
            await ws.send(payload)
            print("> ", end="", flush=True)

async def main():
    print("🚀 Anycast Command Tunnel (v3.0 - Remote Shell)")
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