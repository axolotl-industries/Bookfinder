import os
import re
import asyncio
import httpx
import typer
import ssl
import ebookmeta
import zipfile
from typing import List, Dict, Optional, Tuple, Generator
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn
from urllib.parse import quote, urljoin

"""
BOOKFINDER
----------
Interactive digital library builder.
1. Metadata: OpenLibrary API (English-only filtering + Bibliography).
2. Discovery: Direct Libgen.li scraping + Dynamic Anna's Domain (via Wikipedia).
3. Selection: Interactive menu (Full Bib, Series, or Single Book).
4. Guardrails: 25MB size limit, EPUB verification, Language detection.
"""

# --- Constants ---
OPEN_LIBRARY_BASE_URL = "https://openlibrary.org"
LIBGEN_BASE_URL = "https://libgen.li"
WIKIPEDIA_ANNAS_URL = "https://en.wikipedia.org/wiki/Anna%27s_Archive"
console = Console()

def create_robust_ssl_context():
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        try: ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        except Exception: pass 
        return ctx
    except Exception: return ssl._create_unverified_context()

async def resolve_annas_domain() -> str:
    """Scrapes Wikipedia to find the current active Anna's Archive domain."""
    console.print("[dim]  Resolving current Anna's Archive domain via Wikipedia...[/dim]")
    fallback = "https://annas-archive.gl"
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(WIKIPEDIA_ANNAS_URL)
            if resp.status_code == 200:
                matches = re.findall(r'https?://annas-archive\.[a-z0-9]{2,4}', resp.text)
                if matches:
                    valid = [m for m in matches if not m.endswith('.org')]
                    domain = valid[-1] if valid else matches[-1]
                    console.print(f"[dim]  Found active domain: [bold cyan]{domain}[/bold cyan][/dim]")
                    return domain
    except Exception: pass
    return fallback

# --- Metadata Fetcher Logic ---
class MetadataFetcher:
    def __init__(self):
        self.client = httpx.Client(timeout=20.0, verify=False)

    def search_author(self, name: str) -> Optional[Tuple[str, str]]:
        try:
            clean_name = name.replace(".", " ").strip()
            response = self.client.get(f"{OPEN_LIBRARY_BASE_URL}/search/authors.json", params={"q": clean_name})
            response.raise_for_status()
            data = response.json()
            if not data.get("docs"): return None
            docs = sorted(data["docs"], key=lambda x: x.get("work_count", 0), reverse=True)
            return docs[0]["key"], docs[0].get("name", name)
        except Exception as e:
            console.print(f"[bold red]Error searching author:[/bold red] {e}")
            return None

    def is_probably_english(self, title: str) -> bool:
        """Heuristic check to skip obvious non-English translations."""
        # Common non-English article/stopword patterns that slip through OpenLibrary's 'eng' filter
        non_eng_patterns = [
            r'^una\s+', r'^un\s+', r'^el\s+', r'^la\s+', r'^los\s+', r'^las\s+', # Spanish
            r'^der\s+', r'^die\s+', r'^das\s+', # German
            r'^le\s+', r'^la\s+', r'^les\s+', r'^des\s+', # French
            r'^het\s+', r'^een\s+', # Dutch
            r'\bcorte\b', r'\bniebla\b', r'\bfuria\b', r'\bedição\b' # Specific common translation words
        ]
        t = title.lower()
        if any(re.search(p, t) for p in non_eng_patterns):
            return False
        return True

    def normalize_title(self, title: str) -> str:
        if not title: return ""
        t = title.lower()
        # Strip subtitles and parentheses
        t = t.split(':')[0].split('(')[0]
        # Clean up common prefixes
        t = re.sub(r'^the\s+|^a\s+|^an\s+', '', t)
        # Clean up common cruft suffixes that interfere with deduplication
        t = re.sub(r'\[\d+/\d+\]|\(part \d+\)', '', t)
        t = re.sub(r'[^\w\s]', '', t)
        return " ".join(t.split())

    def get_author_books(self, author_id: str, title_filter: Optional[str] = None, series_filter: Optional[str] = None) -> List[Dict]:
        params = {"author": author_id, "language": "eng", "fields": "title,isbn,first_publish_year,subject,language"}
        if title_filter: params["q"] = title_filter
        
        try:
            response = self.client.get(f"{OPEN_LIBRARY_BASE_URL}/search.json", params=params)
            response.raise_for_status()
            data = response.json()
            
            books = []
            seen_normalized = set()
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

            for doc in data.get("docs", []):
                title = doc.get("title", "")
                if not title: continue
                
                # 1. Title Cruft Check
                t_lower = title.lower()
                if any(re.search(p, t_lower) for p in CRUFT): continue

                # 2. Language Guard (OpenLibrary filter is good, but heuristic is safer)
                if not self.is_probably_english(title): continue
                
                # 3. Subject Filter
                subjects = [s.lower() for s in doc.get("subject", [])]
                NON_WANTED = ["history", "criticism", "manual", "biography", "non-fiction", "nonfiction", "bibliography", "cookbook", "calendar", "comics", "graphic novels", "periodicals", "study guide"]
                WANTED_GENRES = ["fiction", "short stories", "novel", "literature", "science fiction", "fantasy", "speculative fiction", "cyberpunk", "horror", "mystery", "thriller"]

                if subjects:
                    if any(term in s for term in NON_WANTED for s in subjects):
                        if not any(term in s for term in WANTED_GENRES for s in subjects):
                            continue

                if series_filter:
                    s_norm = self.normalize_title(series_filter)
                    if s_norm not in self.normalize_title(title) and not any(s_norm in self.normalize_title(s) for s in subjects):
                        continue

                # 4. Primary Work Verification
                if subjects:
                    is_fiction = any(term in s for term in WANTED_GENRES for s in subjects)
                    if not is_fiction: continue

                # 5. Normalization and Deduplication
                norm = self.normalize_title(title)
                if not norm: continue
                if title_filter and self.normalize_title(title_filter) not in norm: continue

                if norm not in seen_normalized:
                    books.append({
                        "title": title,
                        "isbns": [i for i in doc.get("isbn", []) if len(i) in [10, 13]],
                        "year": doc.get("first_publish_year"),
                        "tags": doc.get("subject", [])[:10]
                    })
                    seen_normalized.add(norm)
            return sorted(books, key=lambda x: (x.get("year") if x.get("year") is not None else 0))
        except Exception as e:
            console.print(f"[bold red]Error fetching bibliography:[/bold red] {e}")
            return []

# --- Scraper Engine Logic ---
class ScraperEngine:
    def __init__(self, headless: bool = True):
        self.headless, self.browser, self.playwright = headless, None, None
        self.annas_base = "https://annas-archive.gl"
        self.client = httpx.AsyncClient(verify=False, timeout=15.0, follow_redirects=True)

    async def start(self):
        self.annas_base = await resolve_annas_domain()
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)

    async def stop(self):
        try:
            if self.browser: await self.browser.close()
            if self.playwright: await self.playwright.stop()
        except Exception: pass
        await self.client.aclose()

    async def _resolve_libgen_ads(self, ads_url: str) -> Optional[str]:
        try:
            resp = await self.client.get(ads_url, timeout=10.0)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                get_link = soup.find('a', href=re.compile(r"get\.php", re.I))
                if get_link:
                    href = get_link['href']
                    return urljoin(ads_url, href) if not href.startswith("http") else href
        except Exception: pass
        return None

    async def get_mirrors(self, author: str, title: str, isbns: List[str]) -> Generator[Tuple[str, str], None, None]:
        # Helper for matching
        norm_title = self.normalize_title(title)
        author_parts = [p for p in self.normalize_title(author).split() if len(p) > 2]

        # 1. Libgen Direct (FAST + Strict English)
        search_title = title.replace("The ", "") if title.startswith("The ") else title
        query = f"{author} {search_title}"
        console.print(f"[dim]  Searching Libgen.li for '{query}'...[/dim]")
        try:
            resp = await self.client.get(f"{LIBGEN_BASE_URL}/index.php", params={"req": query, "res": 25, "filesuns": "all"})
            if resp.status_code == 200:
                rows = BeautifulSoup(resp.text, 'html.parser').find_all('tr')[1:]
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 9:
                        row_author = self.normalize_title(cols[1].get_text())
                        row_title = self.normalize_title(cols[2].get_text())
                        row_lang = cols[6].get_text().lower()
                        row_ext = cols[8].get_text().lower()

                        is_epub = 'epub' in row_ext
                        is_eng = any(l in row_lang for l in ['english', 'eng']) or not row_lang.strip()
                        title_match = norm_title in row_title or row_title in norm_title
                        author_match = any(p in row_author for p in author_parts) if author_parts else True

                        if is_epub and is_eng and title_match and author_match:
                            for l in cols[1].find_all('a', href=True):
                                if 'ads.php' in l['href']:
                                    direct = await self._resolve_libgen_ads(urljoin(LIBGEN_BASE_URL, l['href']))
                                    if direct: yield "Libgen Direct", direct
        except Exception: pass

        # 2. Anna's Archive (Browser Fallback + Strict English)
        queries = [isbns[0]] if isbns else []
        queries.append(f"{title} {author}")
        page = await self.browser.new_page()
        try:
            for q in queries[:2]:
                console.print(f"[dim]  Searching Anna's Archive for '{q}'...[/dim]")
                await page.goto(f"{self.annas_base}/search?q={quote(q)}&ext=epub&lang=en", timeout=25000)
                await asyncio.sleep(2)
                results = BeautifulSoup(await page.content(), 'html.parser').find_all('a', class_='js-vim-focus') or []
                for cand in results[:3]:
                    cand_text = self.normalize_title(cand.get_text())
                    
                    if q == (isbns[0] if isbns else None):
                        match = True
                    else:
                        title_match = norm_title in cand_text or cand_text in norm_title
                        author_match = any(p in cand_text for p in author_parts) if author_parts else True
                        match = title_match and author_match

                    if match:
                        await page.goto(urljoin(self.annas_base, cand['href']), timeout=25000)
                        await asyncio.sleep(2)
                        md5_soup = BeautifulSoup(await page.content(), 'html.parser')
                        libgen = md5_soup.find('a', href=re.compile(r"libgen\.li/ads\.php", re.I))
                        if libgen:
                            direct = await self._resolve_libgen_ads(libgen['href'])
                            if direct: yield "Anna Libgen", direct
                        ipfs = md5_soup.find('a', href=re.compile(r"ipfs", re.I))
                        if ipfs:
                            h = ipfs['href']
                            if h.startswith('ipfs://'): yield "IPFS", f"https://ipfs.io/ipfs/{h.replace('ipfs://', '')}"
                            elif h.startswith('http'): yield "External IPFS", h
        except Exception: pass
        finally: await page.close()

# --- Downloader Logic ---
class Downloader:
    def __init__(self, base_dir: str = ".", max_size_mb: int = 25):
        self.base_dir = os.path.abspath(base_dir)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.ssl_context = create_robust_ssl_context()
        os.makedirs(self.base_dir, exist_ok=True)
        try: os.chmod(self.base_dir, 0o777)
        except: pass

    def is_valid_epub(self, file_path: str) -> bool:
        try:
            if not zipfile.is_zipfile(file_path): return False
            with zipfile.ZipFile(file_path) as z: return 'mimetype' in z.namelist()
        except Exception: return False

    def get_path(self, title: str) -> str:
        safe_title = re.sub(r'[\\/*?:"<>|]', '', title)
        return os.path.join(self.base_dir, f"{safe_title}.epub")

    async def download_from_mirror(self, mirror_name: str, url: str, author_name: str, book_data: Dict) -> bool:
        file_path = self.get_path(book_data["title"])
        for ssl_config in [{"verify": self.ssl_context}, {"verify": False}, {"verify": True}]:
            try:
                async with httpx.AsyncClient(follow_redirects=True, timeout=25.0, **ssl_config) as client:
                    async with client.stream("GET", url) as response:
                        if response.status_code != 200 or "text/html" in response.headers.get("Content-Type", "").lower(): continue
                        size = int(response.headers.get("Content-Length", 0))
                        if size > self.max_size_bytes:
                            console.print(f"[dim yellow]  Mirror skipped: Too large ({size/1024/1024:.1f}MB)[/dim yellow]")
                            return False
                        with Progress(TextColumn("[bold blue]{task.description}"), BarColumn(), DownloadColumn(), console=console, transient=True) as progress:
                            task_id = progress.add_task(f"{mirror_name}...", total=size if size > 0 else None)
                            downloaded_size = 0
                            with open(file_path, "wb") as f:
                                async for chunk in response.aiter_bytes():
                                    f.write(chunk)
                                    downloaded_size += len(chunk)
                                    progress.update(task_id, advance=len(chunk))
                                    if downloaded_size > self.max_size_bytes: break
                
                try: os.chmod(file_path, 0o777)
                except: pass

                if self.is_valid_epub(file_path):
                    try:
                        meta = ebookmeta.get_metadata(file_path)
                        meta.title, meta.author_list_to_string = book_data["title"], author_name
                        if book_data.get("year"): meta.publish_year = str(book_data["year"])
                        ebookmeta.set_metadata(file_path, meta)
                        try: os.chmod(file_path, 0o777)
                        except: pass
                        console.print(f"[dim]  Tagged: {book_data['title']}[/dim]")
                    except Exception: pass
                    return True
                if os.path.exists(file_path): os.remove(file_path)
            except Exception:
                if os.path.exists(file_path): os.remove(file_path)
        return False

# --- CLI Orchestration ---
app = typer.Typer()

async def run_search(author: str, t_q: Optional[str], s_q: Optional[str], dry_run: bool, output: Optional[str], limit: Optional[int]):
    fetcher, scraper, downloader = MetadataFetcher(), ScraperEngine(), Downloader(output or ".")
    results = {"downloaded": [], "failed": []}
    try:
        author_info = fetcher.search_author(author)
        if not author_info: return console.print("[bold red]Author not found.[/bold red]")
        author_id, formal_name = author_info
        books = fetcher.get_author_books(author_id, t_q, s_q)
        if not books: return console.print("[bold red]No matching books found (filtering for English fiction).[/bold red]")
        if limit: books = books[:limit]
        
        table = Table(title=f"Target List: {formal_name}")
        table.add_column("Year", style="cyan"); table.add_column("Title", style="white")
        for b in books: table.add_row(str(b["year"]), b["title"])
        console.print(table)
        if dry_run: return

        await scraper.start()
        for book in books:
            path = downloader.get_path(book["title"])
            if os.path.exists(path) and downloader.is_valid_epub(path):
                console.print(f"[dim]Already exists: {book['title']}[/dim]")
                results["downloaded"].append(book["title"]); continue
            console.print(f"\n[bold blue]Processing:[/bold blue] [white]{book['title']}[/white]")
            success = False
            async for m_name, url in scraper.get_mirrors(formal_name, book["title"], book["isbns"]):
                if await downloader.download_from_mirror(m_name, url, formal_name, book):
                    results["downloaded"].append(book["title"]); success = True; break
            if not success:
                console.print(f"[red]  Failed to find valid download.[/red]")
                results["failed"].append(book["title"])
    except KeyboardInterrupt: console.print("\n[yellow]Interrupted by user.[/yellow]")
    finally:
        await scraper.stop()
        console.print(f"\n[bold green]SUMMARY[/bold green]\nTargeted: {len(books)} | Downloaded: {len(results['downloaded'])}")
        if results["failed"]:
            console.print(f"[bold red]Failed:[/bold red] {len(results['failed'])}")
            for f in results["failed"][:10]: console.print(f" - {f}")
        console.print("="*50)

@app.command()
def search(dry_run: bool = False, output: Optional[str] = None, limit: Optional[int] = None):
    author = typer.prompt("Author Name")
    console.print("\n[bold cyan]Modes:[/bold cyan] 1) Bib, 2) Book, 3) Series")
    mode = typer.prompt("Select", default="1")
    t_q, s_q = (typer.prompt("Title") if mode=="2" else None), (typer.prompt("Series") if mode=="3" else None)
    console.print(Panel(f"[bold blue]Bookfinder:[/bold blue] [green]{author}[/green]"))
    try: asyncio.run(run_search(author, t_q, s_q, dry_run, output, limit))
    except KeyboardInterrupt: pass

if __name__ == "__main__": app()
