from fastapi import FastAPI, HTTPException

app = FastAPI()

BOOKS = [
    {"title": "Title One", "author": "Author One", "category": "Science"},
    {"title": "Title Two", "author": "Author Two", "category": "Science"},
    {"title": "Title Three", "author": "Author Three", "category": "History"},
    {"title": "Title Four", "author": "Author Four", "category": "Math"},
    {"title": "Title Five", "author": "Author Five", "category": "Math"},
    {"title": "Title Six", "author": "Author Two", "category": "Math"},
]

@app.get("/books/byauthor/{author}")
async def fetch_book_by_author(author: str):
    results = [
        book for book in BOOKS if
        book["author"].casefold() == author.casefold()
    ]
    if not results:
        raise HTTPException(status_code=404, detail="author not found")
    return results