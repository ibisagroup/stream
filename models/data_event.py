from models.event_interface import EventInterface
from database import DatabaseManager


class DataEvent(EventInterface):
    __tablename__ = 'DATA_EVENT'

    def canTriggerEvent(self):
        if self.current_data.value != "time":
            if self.is_init:
                self.is_init = False
            self.last_data = self.current_data
            self.current_data = None
        else:
            if int(self.current_data.timestamp) - int(self.last_data.timestamp) > int(self.limit_) and not self.is_init: 
                self.is_init = True


EventInterface.metadata.create_all(DatabaseManager.get_engine())