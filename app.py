import asyncio, json, os, secrets, sys, uvicorn, uuid, time
from fastapi import FastAPI, Depends, HTTPException, Body
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from core import MetadataFetcher, ScraperEngine, Downloader

app = FastAPI()
security = HTTPBasic()

class JobStore:
    def __init__(self):
        self.jobs = {} 
    def add_log(self, job_id, msg):
        if job_id not in self.jobs: self.jobs[job_id] = {'logs': [], 'status': 'running', 'created': time.time()}
        self.jobs[job_id]['logs'].append(msg)
        sys.stdout.write(f"[{job_id}] {msg}\n"); sys.stdout.flush()

JOBS = JobStore()
ADMIN_USER = os.getenv("BOOKFINDER_USER", "admin")
ADMIN_PASS = os.getenv("BOOKFINDER_PASS", "password")

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if not (secrets.compare_digest(credentials.username, ADMIN_USER) and 
            secrets.compare_digest(credentials.password, ADMIN_PASS)):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.username

@app.get("/", response_class=HTMLResponse)
async def index(u: str = Depends(authenticate)):
    with open("static/index.html") as f: return f.read()

@app.get("/search")
async def search(author: str, query: str = None, u: str = Depends(authenticate)):
    fetcher = MetadataFetcher()
    authors = fetcher.search_author(author)
    if not authors: return {"error": "Author not found"}
    # If there's an exact match with high work count, or just one result, we could auto-select,
    # but based on user request, let's return the list for disambiguation.
    return {"authors": authors}

@app.get("/author_books")
async def author_books(author_id: str, author_name: str, query: str = None, u: str = Depends(authenticate)):
    fetcher = MetadataFetcher()
    books = fetcher.get_author_books(author_id, query)
    return {"author": author_name, "books": books}

@app.post("/start_job")
async def start_job(data: dict = Body(...), u: str = Depends(authenticate)):
    job_id = str(uuid.uuid4())
    JOBS.jobs[job_id] = {'logs': [], 'status': 'running', 'created': time.time()}
    asyncio.create_task(run_background_download(job_id, data))
    return {"job_id": job_id}

async def run_background_download(job_id, data):
    def log(m): JOBS.add_log(job_id, m)
    scraper = ScraperEngine(log, data.get('prowlarr_url'), data.get('prowlarr_key'))
    # Use explicit absolute path for Docker volume
    downloader = Downloader("/app/downloads", log)
    await scraper.start()
    try:
        for b in data['books']:
            log(f"PROCESSING: {b['title']}")
            mirrors = await scraper.get_mirrors(data['author'], b['title'], b['isbns'])
            success = False
            for name, url in mirrors:
                if await downloader.download(name, url, data['author'], b['title'], b):
                    log(f"SUCCESS: {b['title']}"); success = True; break
            if not success: log(f"FAILED: {b['title']}")
    finally:
        await scraper.stop()
        if job_id in JOBS.jobs: JOBS.jobs[job_id]['status'] = 'complete'
        log("JOB_COMPLETE")

@app.get("/stream/{job_id}")
async def stream(job_id: str, last_idx: int = 0):
    async def generator():
        idx = last_idx
        while True:
            job = JOBS.jobs.get(job_id)
            if not job: break
            while idx < len(job['logs']):
                yield f"data: {job['logs'][idx]}\n\n"
                idx += 1
            if job['status'] == 'complete': break
            yield ": heartbeat\n\n"
            await asyncio.sleep(2)
    return StreamingResponse(generator(), media_type="text/event-stream")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
