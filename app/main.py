from fastapi import FastAPI
from app.api.admin import endpoints

app = FastAPI(title="My FastAPI Project")

app.include_router(endpoints.router, prefix="/api/admin")