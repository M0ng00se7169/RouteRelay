from infrastructure.repositories.filters.messages import (
    GetAllChatsFilters as GetAllChatsInfrastructureFilters,
    GetMessagesFilters as GetMessagesInfraFilters,
)
from pydantic import BaseModel


class GetMessagesFilters(BaseModel):
    limit: int = 10
    offset: int = 0

    def to_infrastructure(self):
        return GetMessagesInfraFilters(limit=self.limit, offset=self.offset)


class GetAllChatsFilters(BaseModel):
    limit: int = 10
    offset: int = 0

    def to_infrastructure(self):
        return GetAllChatsInfrastructureFilters(limit=self.limit, offset=self.offset)
