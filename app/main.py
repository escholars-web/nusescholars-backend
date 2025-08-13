from fastapi import FastAPI
from app.api.admin import profiles

app = FastAPI(title="NUS E-Scholars Admin Backend")

app.include_router(profiles.router)