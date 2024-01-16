from database import Base
from sqlalchemy import Integer, String, Boolean, Column

from database import DatabaseManager


class Token(Base):
    __tablename__ = 'TOKEN'

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    token: token = Column(String(50), unique=True)
    is_active: bool = Column(Boolean, default=False)

    def __init__(self, **kwargs) -> None:
        self.token: str = kwargs['token']
        self.is_active = True

    def __str__(self) -> str:
        return f"{self.id} {self.token} {self.is_active}"

    def __repr__(self) -> str:
        return f"{self.id} {self.token} {self.is_active}"


Base.metadata.create_all(DatabaseManager.get_engine())