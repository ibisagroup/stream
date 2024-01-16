from models.event_interface import EventInterface

from sqlalchemy import Column, String


class Threshold(EventInterface):
    __tablename__ = 'THRESHOLD' 
    type_: str = Column(String(50))

    def __init__(self, **kwargs) -> None:
        self.type_: str = kwargs.pop("type_")
        super().__init__(**kwargs)

    def canTriggerEvent(self):
        is_active = False
        if self.type_ == ">":
            is_active = float(self.current_data.value) > float(self.limit_)
        elif self.type_ == ">=":
            is_active = float(self.current_data.value) >= float(self.limit_)
        elif self.type_ == "<":
            is_active = float(self.current_data.value) < float(self.limit_)
        elif self.type_ == "<=":
            is_active = float(self.current_data.value) <= float(self.limit_)
        elif self.type_ == "=":
            is_active = float(self.current_data.value) == float(self.limit_)
 
        if not is_active:
            if self.is_init:
                self.is_init = False
            self.last_data = self.current_data
            self.current_data = None
        else:
            if not self.is_init:
                self.is_init = True