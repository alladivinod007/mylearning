# main.py
import uvicorn
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel

from auth import authenticate_user
from circuit_breaker import CircuitBreaker
from queryservice import queryservice_router
from reservationservice import reservationservice_router

app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"],
)

app.include_router(queryservice_router, tags=["queryservice"])
app.include_router(reservationservice_router, tags=["reservationservice"])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
