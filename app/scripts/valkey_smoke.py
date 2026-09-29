"""Live smoke check for ValkeyCacheClient against a real server.

Not part of the suite: the suite is service-free by design. Run manually
against the compose valkey:

    docker compose -f docker_compose/valkey.yaml up -d
    cd app && uv run python scripts/valkey_smoke.py
    docker compose -f docker_compose/valkey.yaml down

It exists because the pytest suite drives ValkeyCacheClient through a
hand-written double: the real client, its pipelines and the two Lua scripts are
exactly the things a double cannot prove.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infrastructure.cache.keys import (
	presence_cache_key,
	relay_lock_key,
)
from infrastructure.cache.valkey import ValkeyCacheClient
from infrastructure.locks.valkey import ValkeyLeaseLock
from infrastructure.presence.valkey import ValkeyPresenceTracker


async def main() -> int:
	url = os.environ.get('VALKEY_URL', 'redis://localhost:6379/0')
	client = ValkeyCacheClient(url=url)
	failures: list[str] = []

	def check(label: str, condition: bool) -> None:
		print(('  ok   ' if condition else '  FAIL ') + label)
		if not condition:
			failures.append(label)

	try:
		# Start from a known state: this script is meant to be re-runnable, and
		# the version counter is permanent by design.
		for key in ('smoke:string', 'smoke:ver', presence_cache_key('smoke:chat'), relay_lock_key()):
			await client.delete(key)

		# strings + TTL
		await client.set('smoke:string', b'hello', ttl_seconds=30)
		check('set/get', await client.get('smoke:string') == b'hello')
		check('exists', await client.exists('smoke:string') is True)
		await client.delete('smoke:string')
		check('delete', await client.get('smoke:string') is None)

		# counter
		check('increment starts at 1', await client.increment('smoke:ver') == 1)
		check('increment increments', await client.increment('smoke:ver') == 2)

		# hash + pipeline TTL
		tracker = ValkeyPresenceTracker(cache=client, ttl_seconds=30)
		await tracker.register('smoke:chat', 'socket-1')
		await tracker.register('smoke:chat', 'socket-2')
		check('hash_count', await tracker.count('smoke:chat') == 2)
		await tracker.remove('smoke:chat', 'socket-1')
		check('hash_delete', await tracker.count('smoke:chat') == 1)
		ttl = await client._get_client().ttl(presence_cache_key('smoke:chat'))
		check('hash TTL armed by the pipeline', 0 < ttl <= 30)

		# lease: SET NX PX + the two Lua scripts
		first = ValkeyLeaseLock(cache=client, key=relay_lock_key(), ttl_seconds=10, holder_id='smoke-1')
		second = ValkeyLeaseLock(cache=client, key=relay_lock_key(), ttl_seconds=10, holder_id='smoke-2')
		check('lease acquire', await first.acquire() is True)
		check('lease is exclusive', await second.acquire() is False)
		check('lease renew (owner)', await first.renew() is True)
		check('lease renew (thief)', await second.renew() is False)
		await first.release()
		check('lease is free after release', await second.acquire() is True)
		# A stale holder must not delete the new owner's lease.
		await first.release()
		check('stale release does not steal', await second.renew() is True)
		await client.delete(relay_lock_key())
	finally:
		await client.aclose()

	# The pool must really be closed (asyncio clients are not GC'd).
	check('client closed', client._client is None)

	print()
	if failures:
		print(f'{len(failures)} check(s) FAILED')
		return 1
	print('all checks passed')
	return 0


if __name__ == '__main__':
	raise SystemExit(asyncio.run(main()))
