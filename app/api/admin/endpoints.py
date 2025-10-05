from fastapi import APIRouter
from app.models.item import Item

router = APIRouter()

@router.get("/")
async def root():
    return {"message": "Welcome to FastAPI"}

@router.get("/items/{item_id}")
async def read_item(item_id: int, q: str = None):
    item = Item(id=item_id, name="Sample Item", description="This is a sample")
    return {"item": item, "query": q}