from locust import FastHttpUser, between, task

API_BASE = '/chat'


class KafkaChatUser(FastHttpUser):
	"""Simulates a chat client creating chats, posting messages, and reading them.

	`wait_time` paces requests so the load is realistic rather than a hammer. One
	chat is created per user in `on_start` and reused for all message writes.
	"""

	wait_time = between(1, 3)

	def on_start(self) -> None:
		# Pre-create a chat once per user, then exercise the message endpoints
		# against that chat for the rest of the run.
		resp = self.client.post(
			API_BASE, json={'name': 'load-test-chat'}, name='POST /chat'
		)
		if resp.status_code == 200:
			self.chat_id = resp.json().get('oid')
		else:
			self.chat_id = None

	@task(3)
	def post_message(self) -> None:
		if not self.chat_id:
			return
		self.client.post(
			f'{API_BASE}/{self.chat_id}/messages',
			json={'text': 'load test message', 'listener_oid': 'loadtest'},
			name='POST /chat/{id}/messages',
		)

	@task(1)
	def list_messages(self) -> None:
		if not self.chat_id:
			return
		self.client.get(
			f'{API_BASE}/{self.chat_id}/messages',
			name='GET /chat/{id}/messages',
		)
