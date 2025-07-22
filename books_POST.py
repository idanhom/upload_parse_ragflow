from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel

from pathlib import Path
import json

app = FastAPI()

BOOKS = [
    {"title": "Title One", "author": "Author One", "category": "Science"},
    {"title": "Title Two", "author": "Author Two", "category": "Science"},
    {"title": "Title Three", "author": "Author Three", "category": "History"},
    {"title": "Title Four", "author": "Author Four", "category": "Math"},
    {"title": "Title Five", "author": "Author Five", "category": "Math"},
    {"title": "Title Six", "author": "Author Two", "category": "Math"},
]

DATA_FILE = Path("books.json")

def load_books() -> list[dict]:
    """
    Read books.json once when the app starts
    If the file is mssing or corrupt, fall back to an empty list
    """
    if not DATA_FILE.exists():
        return []
    try:
        return json.loads(DATA_FILE.read_text())
    except json.JSONDecodeError:
        return []
    
BOOKS: list[dict] = load_books()







