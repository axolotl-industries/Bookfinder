import asyncio, html as htmllib, os, re, secrets, sys, time, uuid
from pathlib import Path
import uvicorn
from typing import Optional
from fastapi import FastAPI, Request, Body, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from core import MetadataFetcher, ScraperEngine, Downloader, NewznabScraper, SabnzbdClient, EmbyAuth, flatten_downloads

app = FastAPI()


# --- Session middleware ---

SESSION_SECRET = os.getenv("SESSION_SECRET") or secrets.token_urlsafe(32)
if not os.getenv("SESSION_SECRET"):
    print("[bookfinder] WARN: SESSION_SECRET is unset; sessions will be invalidated on every "
          "container restart. Set a long random string in docker-compose.yml.", file=sys.stderr)

# Cookies are flagged Secure (HTTPS-only) when SESSION_COOKIE_SECURE is truthy. Default off so
# direct LAN access over http:// still works; turn it on once you're only reaching Bookfinder
# through Cloudflare / a TLS reverse proxy.
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "").lower() in ("1", "true", "yes")

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="bookfinder_session",
    same_site="lax",
    https_only=SESSION_COOKIE_SECURE,
    max_age=7 * 24 * 3600,
)


# --- Auth backend ---

AUTH = EmbyAuth(os.getenv("EMBY_URL", ""))
if not AUTH.server_url:
    print("[bookfinder] WARN: EMBY_URL is unset; no users will be able to log in.", file=sys.stderr)


def current_user(request: Request) -> str:
    user = request.session.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user


# --- Job store ---

class JobStore:
    def __init__(self):
        self.jobs = {}
        self.tasks = {}

    def add_log(self, job_id, msg):
        if job_id not in self.jobs:
            self.jobs[job_id] = {'logs': [], 'status': 'running', 'created': time.time()}
        self.jobs[job_id]['logs'].append(msg)
        sys.stdout.write(f"[{job_id}] {msg}\n"); sys.stdout.flush()


JOBS = JobStore()


# --- Login / logout ---

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    if request.session.get("user"):
        return RedirectResponse("/", status_code=303)
    with open("static/login.html") as f:
        html = f.read()
    err_html = f'<div class="error">{htmllib.escape(error)}</div>' if error else ""
    return html.replace("<!-- ERROR -->", err_html)


@app.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    user = await AUTH.authenticate(username, password)
    if user:
        request.session["user"] = user["name"]
        request.session["is_admin"] = user.get("is_admin", False)
        return RedirectResponse("/", status_code=303)
    print(f"[bookfinder] login failed for username={username!r}", file=sys.stderr)
    await asyncio.sleep(0.5)  # minor speed bump for credential stuffing
    return RedirectResponse("/login?error=Invalid+username+or+password", status_code=303)


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


# --- App routes ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=303)
    with open("static/index.html") as f:
        return HTMLResponse(f.read(), headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})


@app.get("/whoami")
async def whoami(request: Request, u: str = Depends(current_user)):
    return {"user": u, "is_admin": bool(request.session.get("is_admin"))}


@app.get("/search")
async def search(author: str, query: str = None, u: str = Depends(current_user)):
    fetcher = MetadataFetcher()
    try:
        authors = await fetcher.search_author(author)
        if not authors:
            return {"error": "Author not found"}
        return {"authors": authors}
    finally:
        await fetcher.aclose()


@app.get("/author_books")
async def author_books(author_id: str, author_name: str, query: str = None, u: str = Depends(current_user)):
    fetcher = MetadataFetcher()
    try:
        books = await fetcher.get_author_books(author_id, author_name, query)
        return {"author": author_name, "books": books}
    finally:
        await fetcher.aclose()


@app.get("/candidates")
async def candidates(author: str, title: str, u: str = Depends(current_user)):
    usenet = NewznabScraper(os.getenv('PROWLARR_URL'), os.getenv('PROWLARR_KEY'), lambda _: None)
    results = await usenet.search(author, title)
    return {"candidates": results}


@app.post("/start_job")
async def start_job(data: dict = Body(...), u: str = Depends(current_user)):
    job_id = str(uuid.uuid4())
    JOBS.jobs[job_id] = {'logs': ["Initializing job..."], 'status': 'running', 'created': time.time()}
    task = asyncio.create_task(run_background_download(job_id, data))
    JOBS.tasks[job_id] = task
    return {"job_id": job_id}


@app.post("/stop_job/{job_id}")
async def stop_job(job_id: str, u: str = Depends(current_user)):
    task = JOBS.tasks.get(job_id)
    if task:
        task.cancel()
        JOBS.add_log(job_id, "JOB_CANCELLED_BY_USER")
        if job_id in JOBS.jobs:
            JOBS.jobs[job_id]['status'] = 'cancelled'
        return {"status": "ok"}
    return {"error": "Job not found"}


DOWNLOAD_DIR = "/app/downloads"
MAX_USENET_TRIES = 3


def _library_epubs() -> set:
    base = Path(DOWNLOAD_DIR)
    return {p.name for p in base.glob('*.epub')} if base.is_dir() else set()


async def run_background_download(job_id, data):
    def log(m): JOBS.add_log(job_id, m)

    usenet = NewznabScraper(os.getenv('PROWLARR_URL'), os.getenv('PROWLARR_KEY'), log)
    sab = SabnzbdClient(os.getenv('SABNZBD_URL'), os.getenv('SABNZBD_KEY'), log)
    scraper = ScraperEngine(log)
    downloader = Downloader(DOWNLOAD_DIR, log)

    await scraper.start()
    try:
        for b in data['books']:
            log(f"Searching for '{b['title']}'...")
            before = _library_epubs()

            # 1. Usenet — if the user pre-selected a specific NZB, use that directly
            if os.getenv('PROWLARR_URL') and os.getenv('PROWLARR_KEY'):
                if b.get('nzb_url'):
                    nzbs = [{'link': b['nzb_url']}]
                else:
                    nzbs = await usenet.search(data['author'], b['title'])
                for nzb in nzbs[:MAX_USENET_TRIES]:
                    nzo_id = await sab.add_url(nzb['link'], f"{data['author']} - {b['title']}")
                    if not nzo_id:
                        continue
                    while True:
                        status = await sab.check_status(nzo_id)
                        if status in ("completed", "failed", "unknown"):
                            break
                        await asyncio.sleep(5)
                    await sab.delete_from_history(nzo_id)
                    flatten_downloads(DOWNLOAD_DIR, lambda _: None)
                    if _library_epubs() - before:
                        break

            # 2. Mirrors
            if not (_library_epubs() - before):
                mirrors = await scraper.get_mirrors(data['author'], b['title'], b['isbns'])
                for name, url in mirrors:
                    if await downloader.download(name, url, data['author'], b['title'], b):
                        break

            # Check if the specific book we wanted (or at least something new) exists
            safe_t = re.sub(r'[\\/*?:"<>|]', "", b['title']).lower()
            current_files = _library_epubs()
            new_files = current_files - before
            
            success = False
            for f in new_files:
                if safe_t in f.lower():
                    success = True
                    break
            
            if success:
                log(f"SUCCESS: {b['title']}")
            else:
                # Fallback: if we can't be sure it's OUR file, but the downloader returned True
                # or SABnzbd finished, we still trust the process.
                if new_files:
                    log(f"SUCCESS: {b['title']}")
                else:
                    log(f"FAILED: {b['title']}")
    except asyncio.CancelledError:
        log("STOPPING: Job was cancelled.")
        raise
    finally:
        flatten_downloads(DOWNLOAD_DIR, lambda _: None)
        await scraper.stop()
        if job_id in JOBS.jobs:
            if JOBS.jobs[job_id]['status'] == 'running':
                JOBS.jobs[job_id]['status'] = 'complete'
        log("JOB_COMPLETE")
        if job_id in JOBS.tasks: del JOBS.tasks[job_id]


@app.get("/stream/{job_id}")
async def stream(request: Request, job_id: str, last_idx: int = 0):
    # EventSource can't set custom headers, so auth relies on the session cookie the browser sends.
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Unauthorized")

    async def generator():
        idx = last_idx
        while True:
            job = JOBS.jobs.get(job_id)
            if not job: break
            while idx < len(job['logs']):
                yield f"data: {job['logs'][idx]}\n\n"
                idx += 1
            if job['status'] in ['complete', 'cancelled']: break
            yield ": heartbeat\n\n"
            await asyncio.sleep(2)
    return StreamingResponse(generator(), media_type="text/event-stream")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
