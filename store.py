class Store:
    def __init__(self):
        self.data = {}

    def set(self, key: str, value):
        self.data[key] = value

    def get(self, key: str):
        if key in self.data:
            return self.data[key]
        return None
