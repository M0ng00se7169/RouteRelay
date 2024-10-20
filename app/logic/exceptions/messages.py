from dataclasses import dataclass

from logic.exceptions.base import LogicException


@dataclass(eq=False)
class ChatWithThatTitleAlreadyExistsException(LogicException):
    title: str

    @property
    def message(self):
        return f'Chat with that {self.title=} already exists'
