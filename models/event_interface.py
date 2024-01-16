from models.data import Data
from models.token import Token
from database import Base, shared_session
from utilities.constanst import TypeToEvent

from requests.sessions import Session

from sqlalchemy import Integer, String, Boolean, Enum
from sqlalchemy.orm import mapped_column, Mapped


def get_session(token) -> Session:
    """Make this session, set header -> requests.Session"""
    assert token is not None, 'A token was expected'
    headers = { 'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token }
    session = Session()
    session.headers.update(headers)
    session.verify = True
    return session


class EventInterface(Base):
    __abstract__ = True

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True)
    asset: Mapped[str] = mapped_column(String(50))
    metric: Mapped[str] = mapped_column(String(50))
    limit_: Mapped[str] = mapped_column(String(50))
    type_to_event: Mapped[TypeToEvent] = mapped_column(Enum(TypeToEvent))
    to_event: Mapped[str] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)

    def __init__(self, **kwargs) -> None:
        self.id: int = kwargs['id']
        self.name: str = kwargs['name']
        self.asset: str = kwargs['asset']
        self.metric: str = kwargs['metric']
        self.limit_: str = kwargs['limit_']
        self.type_to_event: TypeToEvent = kwargs['type_to_event']
        self.to_event: str = kwargs['to_event']
        self.__dict__["is_init"]: bool = False
        self.last_data: Data = None
        self.current_data: Data = None

    def __str__(self) -> str:
        return f"{self.id} {self.name} {self.is_init}"

    def __repr__(self) -> str:
        return f"{self.id} {self.name} {self.is_init}"

    def __setattr__(self, __name: str, __value: any) -> None:
        if __name == "is_init":
            self.callbackEvent(__value)
        super().__setattr__(__name, __value)

    def callbackEvent(self, value: bool):
        from_time = self.last_data.timestamp
        to_time = self.current_data.timestamp
        token = shared_session.query(Token).first()
        session = get_session(token)
        if token is not None:
            if value:
                if self.type_to_event == TypeToEvent.notification:
                    print(f'SEND NOTIFICATION INIT EVENT, WITH TOKEN: {token}')
                elif self.type_to_event == TypeToEvent.process:
                    print(f'SEND FLOW INIT EVENT, WITH TOKEN: {token}')
            else:
                print(f'Time duration event: {to_time - from_time} sg')
                if self.type_to_event == TypeToEvent.notification:
                    print(f'SEND NOTIFICATION FINISH EVENT, WITH TOKEN: {token}')
                elif self.type_to_event == TypeToEvent.process:
                    print(f'SEND FLOW FINISH EVENT, WITH TOKEN: {token}')
        else:
            print('NO SEND EVENT, WITH TOKEN NO EXIST')

    def canTriggerEvent(self):
        pass