from pydantic import BaseModel

from infrastructure.repositories.filters.messages import GetMessagesFilters as GetMessagesInfraFilters


class GetMessagesFilters(BaseModel):
    limit: int = 10
    offset: int = 0

    def to_infrastructure(self):
        return GetMessagesInfraFilters(limit=self.limit, offset=self.offset)
