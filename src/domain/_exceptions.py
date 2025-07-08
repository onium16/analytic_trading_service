# src/domain/_exceptions.py

class InvalidOrderBookError(Exception):
    """Исключение, возникающее при недопустимом формате книги ордеров."""
    def __init__(self, message: str = "Invalid order book format"):
        self.message = message
        super().__init__(self.message)

class InvalidPriceError(Exception):
    """Исключение, возникающее при недопустимой цене."""
    def __init__(self, message: str = "Price must be positive"):
        self.message = message
        super().__init__(self.message)

class InvalidVolumeError(Exception):
    """Исключение, возникающее при недопустимом объёме."""
    def __init__(self, message: str = "Volume must be positive"):
        self.message = message
        super().__init__(self.message)