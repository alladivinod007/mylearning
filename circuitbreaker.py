# circuit_breaker.py
from typing import Callable

class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int, timeout: int, recovery_timeout: int):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.recovery_timeout = recovery_timeout

        self.state = "CLOSED"
        self.fail_count = 0
        self.next_available_time = None

    def call(self, func: Callable) -> Any:
        if self.state == "CLOSED":
            try:
                return func()
            except Exception as e:
                self.fail_count += 1
                if self.fail_count >= self.failure_threshold:
                    self.state = "OPEN"
                    self.next_available_time = time.time() + self.timeout
                raise e
        elif self.state == "OPEN":
            if time.time() >= self.next_available_time:
                self.state = "HALF_OPEN"
            else:
                raise CircuitBreakerOpenException(self.name)

        try:
            return func()
        except Exception as e:
            self.state = "OPEN"
            self.next_available_time = time.time() + self.timeout
            raise e
        else:
            self.state = "CLOSED"
            self.fail_count = 0
            return result

class CircuitBreakerOpenException(Exception):
    def __init__(self, name: str):
        super().__init__(f"Circuit breaker {name} is open.")
