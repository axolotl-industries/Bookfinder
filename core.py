import os
import re
import asyncio
import httpx
import ssl
import ebookmeta
import zipfile
import sys
from typing import List, Dict, Optional, Tuple, Generator, Callable
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser
from urllib.parse import quote, urljoin

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def normalize_text(text: str) -> str:
    if not text: return ""
    t = text.lower()
    # Strip subtitles and parentheses
    t = t.split(':')[0].split('(')[0]
    # Clean up common prefixes
    t = re.sub(r'^the\s+|^a\s+|^an\s+', '', t)
    # Clean up common cruft suffixes that interfere with deduplication
    t = re.sub(r'\[\d+/\d+\]|\(part \d+\)', '', t)
    t = re.sub(r'[^\w\s]', '', t)
    return " ".join(t.split())

def create_robust_ssl_context():
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx
    except:
        return ssl._create_unverified_context()

async def resolve_annas_domain(log_func: Callable) -> str:
    mirrors = ["https://annas-archive.se", "https://annas-archive.li", "https://annas-archive.gs"]
    for m in mirrors:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                if (await client.head(m)).status_code < 400: return m
        except: continue
    return "https://annas-archive.gl"

class MetadataFetcher:
    def __init__(self):
        self.client = httpx.Client(timeout=20.0, verify=False, headers={"User-Agent": UA})

    def search_author(self, name: str) -> Optional[Tuple[str, str]]:
        try:
            clean_name = name.replace(".", " ").strip().lower()
            response = self.client.get(f"https://openlibrary.org/search/authors.json", params={"q": clean_name})
            docs = response.json().get("docs", [])
            if not docs: return None
            for doc in docs[:10]:
                if doc.get("name", "").lower() == clean_name: return doc["key"], doc.get("name")
            best = sorted(docs[:5], key=lambda x: x.get("work_count", 0), reverse=True)[0]
            return best["key"], best.get("name")
        except: return None

    def get_author_books(self, author_id: str, query: Optional[str] = None) -> List[Dict]:
        books, seen, page = [], set(), 1
        # Robust filtering for omnibuses, anthologies, non-fiction, and non-primary works
        CRUFT = [
            r"\bbox set\b", r"\btrilogy\b", r"\bomnibus\b", r"\bselections\b",
            r"\bsummary\b", r"\banalysis\b", r"\bstudy guide\b", r"\bcompanion\b",
            r"\btrivia\b", r"\bunofficial\b", r"\bnotebook\b", r"\bdiary\b",
            r"\bjournal\b", r"\bcalendar\b", r"\bcatalog\b", r"\bbin\b",
            r"\bpack\b", r"\bx\d+\b", r"\bd/b\b", r"\bÚ10\b",
            r"\bvol\.\s*\d+\b", r"\bvolume\s*\d+\b", r"\bissue\b", r"\bmagazine\b",
            r"\breader\b", r"\bcollege\b", r"\bcourse\b", r"\btextbook\b",
            r"\bcookbook\b", r"\bkitchen\b", r"\bcomic\b", r"\bgraphic novel\b",
            r"\bmanga\b", r"\bedited by\b", r"\bvarious authors\b",
            r"\[\d+/\d+\]", r"\(part \d+\)",
            r"\bthe best (american|british|science|horror|mystery|sports)\b",
            r" / ", r" & ", r" ; "
        ]
        
        while True:
            try:
                resp = self.client.get("https://openlibrary.org/search.json", params={"author": author_id, "language": "eng", "fields": "title,isbn,first_publish_year,subject", "page": page})
                docs = resp.json().get("docs", [])
                if not docs: break
                for doc in docs:
                    title = doc.get("title", "")
                    if not title: continue
                    
                    # 1. Title Cruft Check
                    t_lower = title.lower()
                    if any(re.search(p, t_lower) for p in CRUFT): continue
                    
                    # 2. Subject Filter
                    subjects = [s.lower() for s in doc.get("subject", [])]
                    NON_WANTED = ["history", "criticism", "manual", "biography", "non-fiction", "nonfiction", "bibliography", "cookbook", "calendar", "comics", "graphic novels", "periodicals", "study guide"]
                    if subjects and any(term in s for term in NON_WANTED for s in subjects):
                        continue
                        
                    # 3. Primary Work Verification (MUST be fiction or short stories if subjects exist)
                    if subjects:
                        is_fiction = any(term in s for term in ["fiction", "short stories", "novel", "literature"] for s in subjects)
                        if not is_fiction: continue

                    # 4. Normalization and Deduplication
                    norm = normalize_text(title)
                    if not norm: continue
                    if query and normalize_text(query) not in norm: continue
                    
                    if norm not in seen:
                        books.append({"title": title, "isbns": [i for i in doc.get("isbn", []) if len(i) in [10, 13]], "year": doc.get("first_publish_year")})
                        seen.add(norm)
                if len(docs) < 100: break
                page += 1
            except: break
        return sorted(books, key=lambda x: (x.get("year") or 0))

class ScraperEngine:
    def __init__(self, log_func: Callable, p_url: str = None, p_key: str = None):
        self.log, self.p_url, self.p_key = log_func, p_url, p_key
        self.browser, self.playwright, self.annas_base = None, None, ""
        self.client = httpx.AsyncClient(verify=False, timeout=20.0, follow_redirects=True, headers={"User-Agent": UA})

    async def start(self):
        self.annas_base = await resolve_annas_domain(self.log)
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)

    async def stop(self):
        try:
            if self.browser: await self.browser.close()
            if self.playwright: await self.playwright.stop()
        except: pass
        await self.client.aclose()

    async def get_mirrors(self, author: str, title: str, isbns: List[str]) -> List[Tuple[str, str]]:
        """Returns a list of mirrors instead of yielding to prevent GeneratorExit issues."""
        mirrors = []
        page = await self.browser.new_page()
        
        clean_title = re.sub(r'^the\s+|^a\s+|^an\s+', '', title.lower())
        queries = []
        if isbns: queries.append(isbns[0])
        queries.append(f"{author} {clean_title}")

        try:
            for q in queries:
                # 1. Libgen
                self.log(f"Searching Libgen for '{q}'...")
                try:
                    await page.goto(f"https://libgen.li/index.php?req={quote(q)}&res=25&filesuns=all", timeout=30000)
                    soup = BeautifulSoup(await page.content(), 'html.parser')
                    for r in soup.find_all('tr')[1:]:
                        if 'epub' in r.get_text().lower() and normalize_text(title) in normalize_text(r.get_text()):
                            ads = r.find('a', href=re.compile(r"ads\.php"))
                            if ads:
                                direct = await self._resolve_mirror(urljoin("https://libgen.li", ads['href']), page)
                                if direct: mirrors.append(("Libgen", direct))
                except: pass

                # 2. Anna's
                self.log(f"Searching Anna's for '{q}'...")
                try:
                    await page.goto(f"{self.annas_base}/search?q={quote(q)}&ext=epub&lang=en", timeout=30000)
                    results = BeautifulSoup(await page.content(), 'html.parser').select('a[href*="/md5/"]')
                    for cand in results[:2]:
                        if q != (isbns[0] if isbns else ""):
                            if normalize_text(title) not in normalize_text(cand.get_text()): continue
                        
                        await page.goto(urljoin(self.annas_base, cand['href']), timeout=30000)
                        msoup = BeautifulSoup(await page.content(), 'html.parser')
                        lg = msoup.find('a', href=re.compile(r"libgen\.li/ads\.php"))
                        if lg:
                            direct = await self._resolve_mirror(lg['href'], page)
                            if direct: mirrors.append(("Anna Libgen", direct))
                        ipfs = msoup.find('a', href=re.compile(r"ipfs"))
                        if ipfs and 'ipfs://' in ipfs['href']:
                            mirrors.append(("IPFS", f"https://ipfs.io/ipfs/{ipfs['href'].split('ipfs://')[1]}"))
                except: pass
        finally: 
            await page.close()
        return mirrors

    async def _resolve_mirror(self, url: str, page: Browser) -> Optional[str]:
        try:
            await page.goto(url, timeout=15000)
            link = BeautifulSoup(await page.content(), 'html.parser').find('a', href=re.compile(r"get\.php|/get/|download", re.I))
            if link: return urljoin(url, link['href'])
        except: pass
        return None

class Downloader:
    def __init__(self, base_dir: str, log_func: Callable):
        self.base_dir = os.path.abspath(base_dir)
        self.log = log_func
        self.ssl_ctx = create_robust_ssl_context()
        os.makedirs(self.base_dir, exist_ok=True)
        # Ensure base directory itself is accessible
        try: os.chmod(self.base_dir, 0o777)
        except: pass

    async def download(self, mirror: str, url: str, author: str, title: str, book_data: Dict) -> bool:
        # Save directly to base_dir, no author subfolders
        safe_title = re.sub(r'[\\/*?:"<>|]', "", title)
        path = os.path.join(self.base_dir, f"{safe_title}.epub")

        for cfg in [{"verify": self.ssl_ctx}, {"verify": False}]:
            try:
                async with httpx.AsyncClient(follow_redirects=True, timeout=60.0, headers={"User-Agent": UA}, **cfg) as client:
                    async with client.stream("GET", url) as resp:
                        if resp.status_code != 200 or "text/html" in resp.headers.get("Content-Type", "").lower(): continue
                        size = int(resp.headers.get("Content-Length", 0))
                        if size > 0 and (size < 10000 or size > 40*1024*1024): continue
                        
                        self.log(f"Downloading from {mirror}...")
                        with open(path, "wb") as f:
                            async for chunk in resp.aiter_bytes(): f.write(chunk)
                            f.flush(); os.fsync(f.fileno())
                
                # Set permissions to 777 immediately after download
                try: os.chmod(path, 0o777)
                except: pass

                if zipfile.is_zipfile(path):
                    with zipfile.ZipFile(path) as z:
                        if 'mimetype' in z.namelist():
                            try:
                                meta = ebookmeta.get_metadata(path)
                                meta.title, meta.author_list_to_string = title, author
                                ebookmeta.set_metadata(path, meta)
                                # Re-apply 777 after metadata tagging (some libs might recreate the file)
                                try: os.chmod(path, 0o777)
                                except: pass
                            except: pass
                            self.log(f"Saved to: {path}")
                            return True
                if os.path.exists(path): os.remove(path)
            except:
                if os.path.exists(path): os.remove(path)
        return False
