class Data():
    def __init__(self, timestamp: int, name: str, value: str) -> None:
        self.timestamp: int = timestamp
        self.name: str = name
        self.value: str = value

    def __str__(self) -> str:
        return f"{self.timestamp} {self.name} {self.value}"

    def __repr__(self) -> str:
        return f"{self.timestamp} {self.name} {self.value}"