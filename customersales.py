# customersales.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from circuit_breaker import CircuitBreaker
from cache import Cache
from customersales_client import customersales_client

customersales_router = APIRouter()

class CustomerSale(BaseModel):
    sale_id: int
    customer_id: int
    total_amount: float

customersales_cache = Cache("customersales_cache")

customersales_circuit_breaker = CircuitBreaker(
    name="customersales_circuit_breaker",
    failure_threshold=5,
    timeout=10,
    recovery_timeout=30,
)

@customersales_router.get("/customersales/{customer_id}", response_model=CustomerSale)
async def get_customer_sale(customer_id: int):
    current_user = authenticate_user()

    if current_user is None:
        raise HTTPException(status_code=401, detail="Unauthorized")

    customersale = await customersales_cache.get(customer_id)

    if customersale is None:
        customersale = await customersales_circuit_breaker.call(
            lambda: customersales_client.get_customersale(customer_id)
        )
        await customersales_cache.set(customer_id, customersale)

    return CustomerSale(customersale=customersale)
