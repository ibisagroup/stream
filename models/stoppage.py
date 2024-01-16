from models.event_interface import EventInterface


class Stoppage(EventInterface):
    __tablename__ = 'STOPPAGE'

    def canTriggerEvent(self):
        if self.last_data.value != self.current_data.value:
            if self.is_init:
                self.is_init = False
            self.last_data = self.current_data
            self.current_data = None
        else:
            if int(self.current_data.timestamp) - int(self.last_data.timestamp) > int(self.limit_) and not self.is_init:   
                self.is_init = True