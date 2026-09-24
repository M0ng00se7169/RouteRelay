import logging
import uuid

from faker import Faker
from locust import (
    between,
    FastHttpUser,
    task,
)


logger = logging.getLogger(__name__)

API_BASE = '/chat'
fake = Faker()


class KafkaChatUser(FastHttpUser):
	"""Simulates a chat client creating chats, posting messages, and reading them.

	`wait_time` paces requests so the load is realistic rather than a hammer. One
	chat is created per user in `on_start` and reused for all message writes.
	"""

	wait_time = between(1, 3)

	def on_start(self) -> None:
		# The API expects {'title': ...} and answers 201 Created. Titles must be
		# unique (duplicates are rejected with 400), so each user mints one with
		# a random suffix; faker supplies the readable part.
		for attempt in range(3):
			title = f'load-test-{fake.word()}-{uuid.uuid4().hex[:8]}'
			resp = self.client.post(
				f'{API_BASE}/',
				json={'title': title},
				name='POST /chat/',
			)
			if resp.status_code == 201:
				self.chat_id = resp.json().get('oid')
				return
			logger.warning(
				'Chat creation attempt %s failed: %s %s',
				attempt + 1, resp.status_code, resp.text[:200],
			)
		self.chat_id = None
		logger.error('Giving up chat creation; this user will not generate load')

	@task(3)
	def post_message(self) -> None:
		if not self.chat_id:
			return
		# CreateMessageSchema accepts {'text': ...} only.
		self.client.post(
			f'{API_BASE}/{self.chat_id}/messages',
			json={'text': fake.sentence()},
			name='POST /chat/{id}/messages',
		)

	@task(1)
	def list_messages(self) -> None:
		if not self.chat_id:
			return
		# Trailing slash matters: the route is registered as /messages/, and
		# hitting it exactly avoids a 307 redirect on every request.
		self.client.get(
			f'{API_BASE}/{self.chat_id}/messages/',
			name='GET /chat/{id}/messages/',
		)
