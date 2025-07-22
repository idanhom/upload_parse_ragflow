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

# ### 1. **Return All Books in a Specific Category and by a Specific Author**

# **Endpoint Design**: Combine path and query parameters.
# **Goal**: Fetch books where both the author and category match.

@app.get("/books/{author}")
async def get_books_by_author_category(author: str, category: str):
    return [
        book for book in BOOKS if
        book["author"].casefold() == author.casefold()
        and book["category"].casefold() == category.casefold()
    ]
# ---

# ### 2. **Return a List of Unique Categories**

# **Endpoint Design**: Use a simple path (e.g., `/categories`).
# **Goal**: Extract and return a list of all unique book categories.

@app.get("/categories")
async def get_unique_categories():
    return sorted({book["category"] for book in BOOKS})



# ---

# ### 3. **Return the First Book Matching a Partial Title Match**

# **Endpoint Design**: Use a query parameter like `?search=some_word`.
# **Goal**: Find and return the **first book** where the title contains the keyword.

@app.get("/books/search/")
async def find_first_title(search: str):
    for book in BOOKS:
        if search.casefold() in book["title"].casefold():
            return book
    raise HTTPException(status_code=404, detail="book not found")


# ---

# ### 4. **Return Whether a Book Exists Based on Title**

# **Endpoint Design**: Use a query parameter for the title.
# **Goal**: Return `{"exists": True}` or `{"exists": False}` depending on whether a book with the given title exists.

@app.get("/books/search/")
async def is_title_existing(search: str):
    for book in BOOKS:
        if search.casefold() == book["title"].casefold():
            return {"exists": True}
        else:
            return {"exists": False}



# ---

# ### 5. **Return All Books Sorted Alphabetically by Title**

# **Endpoint Design**: Simple path endpoint (e.g., `/books/sorted`).
# **Goal**: Return all books sorted by their title in ascending order.
@app.get("/books/sorted/")
async def book_sorted():
    return sorted([book["title"] for book in BOOKS], reverse=True)



# ---

