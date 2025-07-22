#!/usr/bin/env python3
"""
Upload local files to a RAGFlow dataset and parse them,
with safer secrets, better logging, and lower memory use.
"""

from __future__ import annotations

import os
import time
import logging
from pathlib import Path
from typing import Iterable, List, Dict

from ragflow_sdk import RAGFlow

# ─────────────────────────── Configuration ────────────────────────────

API_KEY       = os.getenv("RAGFLOW_API_KEY")           # ← export this!
BASE_URL      = "http://127.0.0.1:9380"
DATASET_NAME  = "Chemistry"
SOURCE_FOLDER = Path("Papers")                         # pathlib!
BATCH_SIZE    = 4                                      # files per upload
PAGE_SIZE     = 100                                    # list_documents
INITIAL_BACKOFF = 1                                    # seconds
MAX_BACKOFF     = 16                                   # seconds

if not API_KEY:
    raise EnvironmentError(
        "Missing environment variable RAGFLOW_API_KEY. "
        "Run:  export RAGFLOW_API_KEY='ragflow-xxxxxxxx'"
    )

# ───────────────────────────── Logging ────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
log = logging.getLogger(__name__)

# ─────────────────────────── Helper Functions ─────────────────────────

def get_rag_client(api_key: str, base_url: str) -> RAGFlow:
    return RAGFlow(api_key=api_key, base_url=base_url)


def find_dataset(client: RAGFlow, name: str):
    ds = client.list_datasets(name=name, page=1, page_size=PAGE_SIZE)
    if not ds:
        raise RuntimeError(f"Dataset '{name}' not found.")
    return ds[0]


def gather_file_paths(root_folder: Path) -> List[Path]:
    """Return a flat list of all files under *root_folder*."""
    return [p for p in root_folder.rglob("*") if p.is_file()]


def build_document(path: Path, root_folder: Path) -> Dict:
    """Create the upload payload for a single file."""
    return {
        "display_name": path.relative_to(root_folder).as_posix(),
        "blob": path.read_bytes(),                      # read on-demand
    }


def stream_documents(dataset, page_size: int = PAGE_SIZE) -> Iterable:
    """Yield **all** documents in the dataset (pagination handled)."""
    page = 1
    while True:
        docs = dataset.list_documents(page=page, page_size=page_size)
        if not docs:
            break
        yield from docs
        page += 1


def upload_in_batches(dataset, file_paths: List[Path], batch_size: int, root_folder: Path):
    """Read and upload files batch-by-batch to keep memory usage low."""
    total = len(file_paths)
    for i in range(0, total, batch_size):
        batch_paths = file_paths[i : i + batch_size]
        docs: List[Dict] = []

        # Build payload lazily
        for path in batch_paths:
            try:
                docs.append(build_document(path, root_folder))
            except Exception as e:
                log.error("Failed to read '%s': %s", path, e)

        if not docs:
            continue

        try:
            dataset.upload_documents(docs)
            log.info("✅ Uploaded batch %d (%d files)",
                     i // batch_size + 1, len(docs))
        except Exception as e:
            log.error("❌ Failed batch %d: %s", i // batch_size + 1, e)


def parse_sequentially(dataset):
    """Parse each document, skipping ones already done, with back-off."""
    idx = 0
    for doc in stream_documents(dataset):
        idx += 1

        if doc.run == "DONE":
            log.info("[%d] Skipping '%s' (already parsed)", idx, doc.name)
            continue

        log.info("[%d] Parsing '%s' (ID=%s)…", idx, doc.name, doc.id)
        try:
            dataset.async_parse_documents([doc.id])
        except Exception as e:
            log.error("Unable to queue parse for '%s': %s", doc.name, e)
            continue

        # Exponential back-off polling
        backoff = INITIAL_BACKOFF
        while True:
            try:
                refreshed = dataset.list_documents(id=doc.id)[0]
            except Exception as e:
                log.error("Polling error for '%s': %s", doc.name, e)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            status = refreshed.run
            if status == "DONE":
                log.info("✓ '%s' parsed.", doc.name)
                break
            if status == "FAIL":
                log.error("Parsing failed for '%s'", doc.name)
                break

            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

# ───────────────────────────── Main Flow ──────────────────────────────

def main():
    client  = get_rag_client(API_KEY, BASE_URL)
    dataset = find_dataset(client, DATASET_NAME)

    # 1. Existing uploads (by display_name)
    existing = {doc.name for doc in stream_documents(dataset)}
    log.info("Found %d already-uploaded documents.", len(existing))

    # 2. Gather local files
    file_paths = gather_file_paths(SOURCE_FOLDER)

    # 3. Filter out duplicates
    to_upload = [p for p in file_paths if p.relative_to(SOURCE_FOLDER).as_posix() not in existing]
    log.info("Preparing to upload %d new documents.", len(to_upload))

    # 4. Upload new documents in batches
    if to_upload:
        upload_in_batches(dataset, to_upload, BATCH_SIZE, SOURCE_FOLDER)
    else:
        log.info("No new documents to upload.")

    # 5. Parse sequentially with exponential back-off
    parse_sequentially(dataset)


if __name__ == "__main__":
    main()
