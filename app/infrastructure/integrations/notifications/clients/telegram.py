from dataclasses import dataclass

from httpx import AsyncClient

from infrastructure.integrations.notifications.clients.base import BaseNotificationClient
from infrastructure.integrations.notifications.dtos import Notification


@dataclass
class TelegramNotificationClient(BaseNotificationClient):
    bot_token: str
    chat_id: str
    http_client: AsyncClient
    send_url: str

    async def _format_notification(self, notification: Notification) -> str:
        return f'{notification.title}\n{notification.text}\n'

    async def send(self, notification: Notification) -> None:
        await self.http_client.get(
            url=f"{self.send_url}/bot{self.bot_token}/sendMessage",
            params={
                'chat_id': self.chat_id,
                'text': await self._format_notification(notification=notification),
            },
        )
