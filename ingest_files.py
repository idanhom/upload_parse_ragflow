import os
import time
from ragflow_sdk import RAGFlow

# ------------------------------
# Configuration
# ------------------------------

API_KEY       = "ragflow-M5ZDg4NGVjNjUyMTExZjBiYmQxN2U3Mj"
BASE_URL      = "http://127.0.0.1:9380"
DATASET_NAME  = "Chemistry"
SOURCE_FOLDER = "Papers"
BATCH_SIZE    = 4       # number of documents per upload batch
PAGE_SIZE     = 100     # number of documents to request per page when listing
POLL_INTERVAL = 1       # seconds between parse status checks

# ------------------------------
# Helper Functions
# ------------------------------

def get_rag_client(api_key: str, base_url: str) -> RAGFlow:
    return RAGFlow(api_key=api_key, base_url=base_url)


def find_dataset(client: RAGFlow, name: str):
    ds = client.list_datasets(name=name, page=1, page_size=PAGE_SIZE)
    if not ds:
        raise RuntimeError(f"Dataset '{name}' not found.")
    return ds[0]


def gather_file_paths(root_folder: str) -> list[str]:
    paths = []
    for dirpath, _, filenames in os.walk(root_folder):
        for fn in filenames:
            paths.append(os.path.join(dirpath, fn))
    return paths


def build_document_payload(paths: list[str], root_folder: str) -> list[dict]:
    docs = []
    for path in paths:
        display_name = os.path.relpath(path, root_folder)
        with open(path, 'rb') as f:
            blob = f.read()
        docs.append({'display_name': display_name, 'blob': blob})
    return docs


def stream_documents(dataset, page_size: int = PAGE_SIZE):
    page = 1
    while True:
        docs = dataset.list_documents(page=page, page_size=page_size)
        if not docs:
            break
        for doc in docs:
            yield doc
        page += 1


def upload_in_batches(dataset, documents: list[dict], batch_size: int):
    total = len(documents)
    for i in range(0, total, batch_size):
        batch = documents[i:i+batch_size]
        try:
            dataset.upload_documents(batch)
            print(f"✅ Uploaded batch {i//batch_size + 1} ({len(batch)} files)")
        except Exception as e:
            print(f"❌ Failed batch {i//batch_size + 1}: {e}")


def parse_sequentially(dataset):
    idx = 0
    for doc in stream_documents(dataset):
        idx += 1
        # skip already parsed
        if doc.run == 'DONE':
            print(f"[{idx}] Skipping '{doc.name}' (already parsed)")
            continue

        print(f"[{idx}] Parsing '{doc.name}' (ID={doc.id})...")
        dataset.async_parse_documents([doc.id])

        # poll until done
        while True:
            refreshed = dataset.list_documents(id=doc.id)[0]
            status = refreshed.run
            if status == 'DONE':
                print(f"✓ '{doc.name}' parsed.")
                break
            if status == 'FAIL':
                raise RuntimeError(f"Parsing failed for {doc.name}")
            time.sleep(POLL_INTERVAL)

# ------------------------------
# Main Workflow
# ------------------------------

def main():
    client = get_rag_client(API_KEY, BASE_URL)
    dataset = find_dataset(client, DATASET_NAME)

    # 1. Determine existing uploads by display_name
    existing = {doc.name for doc in stream_documents(dataset)}
    print(f"Found {len(existing)} already uploaded documents.")

    # 2. Gather local files and build payloads
    file_paths = gather_file_paths(SOURCE_FOLDER)
    all_docs = build_document_payload(file_paths, SOURCE_FOLDER)

    # 3. Filter out already uploaded
    to_upload = [d for d in all_docs if d['display_name'] not in existing]
    print(f"Preparing to upload {len(to_upload)} new documents.")

    # 4. Upload new documents in batches
    if to_upload:
        upload_in_batches(dataset, to_upload, BATCH_SIZE)
    else:
        print("No new documents to upload.")

    # 5. Parse sequentially
    parse_sequentially(dataset)

if __name__ == '__main__':
    main()