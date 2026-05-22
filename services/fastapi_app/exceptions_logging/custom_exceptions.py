######################################################
# custom_exceptions.py
#
# All custom application exceptions
#######################################################

class BaseAppException(Exception):
    """
    All custom application exceptions should inherit from this class.
    Contains an error message and an HTTP status code.
    """
    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class MinIOException(BaseAppException):
    # Raised when a MinIO operation fails
    def __init__(self, message: str):
        super().__init__(message, status_code=500)


class KafkaException(BaseAppException):
    # Raised when a Kafka-related operation fails
    def __init__(self, message: str):
        super().__init__(message, status_code=500)


class DataBaseException(BaseAppException):
    # Raised when a database operation fails
    def __init__(self, message: str):
        super().__init__(message, status_code=500)