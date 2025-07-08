# src/application/_exceptions.py

class InvalidTradeResultError(Exception):
    """Исключение, возникающее при некорректном формате данных о сделке."""
    def __init__(self, message: str = "Invalid trade result format", details: dict | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)
        
class InsufficientDataError(Exception):
    """Выбрасывается, когда данных для бэктеста недостаточно."""
    def __init__(self, message: str = "Недостаточно данных для бэктеста", details: dict | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def __str__(self):
        base = self.message
        if self.details:
            return f"{base} | Details: {self.details}"
        return base