"""WS fan-out smoke test (final hop of the README flow diagram).

Creates a chat, connects a WebSocket client to the room, posts a message and
verifies the client receives it via the broker fan-out; then deletes the chat
and checks the deleted-room behavior. Prints PASS/FAIL per step.
"""

import asyncio
import json
import sys
import uuid

import httpx
import websockets


BASE = 'http://localhost:8000'
RECV_TIMEOUT = 30


async def main() -> int:
	results = []

	async with httpx.AsyncClient(base_url=BASE, timeout=15, trust_env=False) as client:
		# 1. Create a chat
		r = await client.post('/chat/', json={'title': f'ws-smoke-{uuid.uuid4().hex[:8]}'})
		results.append(('create chat', r.status_code == 201, f'{r.status_code} {r.text[:80]}'))
		chat_oid = r.json()['oid']

		# 2. Connect a WebSocket client to the room (exercises accept_connection)
		ws_uri = f'ws://localhost:8000/chats/{chat_oid}/'
		async with websockets.connect(ws_uri) as ws:
			greeting = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
			results.append(('greeting', greeting == 'You are now connected!', repr(greeting)))

			# 3. Post a message while the socket is open
			r = await client.post(f'/chat/{chat_oid}/messages', json={'text': 'ws smoke message'})
			results.append(('post message', r.status_code == 201, f'{r.status_code}'))

			# 4. Fan-out: broker -> consumer loop -> send_all -> this socket.
			# send_all transmits event.message.encode() — a binary frame — so the
			# client sees bytes (the greeting is a text frame; decode uniformly).
			fanout = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
			fanout_text = fanout.decode() if isinstance(fanout, bytes) else fanout
			results.append(('fan-out message', fanout_text == 'ws smoke message', repr(fanout)))
		# leaving the context sends a close frame -> server removes the connection

		# 5. Delete the chat, then reconnect to observe the deleted-room path
		r = await client.delete(f'/chat/{chat_oid}/')
		results.append(('delete chat', r.status_code == 204, f'{r.status_code}'))
		try:
			async with websockets.connect(ws_uri) as ws2:
				try:
					msg2 = json.loads(await asyncio.wait_for(ws2.recv(), timeout=RECV_TIMEOUT))
					ok = msg2.get('error') is not None or msg2.get('message') == 'Chat has been deleted'
					results.append(('deleted-room notice', ok, repr(msg2)))
				except websockets.ConnectionClosed:
					results.append(('deleted-room notice', True, 'connection closed by server'))
		except websockets.exceptions.InvalidStatus:
			results.append(('deleted-room notice', True, 'handshake rejected by server'))

	print()
	all_ok = True
	for name, ok, detail in results:
		all_ok = all_ok and ok
		print(f"{'PASS' if ok else 'FAIL'}  {name:22s} {detail}")
	print('\nSMOKE TEST:', 'ALL PASS' if all_ok else 'FAILURES PRESENT')
	return 0 if all_ok else 1


if __name__ == '__main__':
	sys.exit(asyncio.run(main()))
