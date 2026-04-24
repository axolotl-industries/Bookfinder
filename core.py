import os
import re
import json
import asyncio
import httpx
import ssl
import ebookmeta
import zipfile
import sys
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional, Tuple, Generator, Callable
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser
from urllib.parse import quote, urljoin

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def normalize_text(text: str) -> str:
    if not text: return ""
    t = text.lower()
    # Replace dots, underscores, and common punctuation with spaces BEFORE stripping
    t = re.sub(r'[\._\-]', ' ', t)
    # Strip subtitles and parentheses
    t = t.split(':')[0].split('(')[0]
    # Clean up common prefixes
    t = re.sub(r'^the\s+|^a\s+|^an\s+', '', t)
    # Clean up common cruft suffixes
    t = re.sub(r'\[\d+/\d+\]|\(part \d+\)', '', t)
    # Remove remaining non-alphanumeric (except spaces)
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

class NewznabScraper:
    def __init__(self, api_url: str, api_key: str, log_func: Callable):
        # Normalise to the indexer root. Strip query string, trailing slashes, and a trailing /api.
        base = api_url.strip().split('?')[0].rstrip('/')
        if base.endswith('/api'):
            base = base[:-4]
        self.api_url = base
        self.api_key = api_key.strip()
        self.log = log_func

    async def search(self, author: str, title: str) -> List[Dict]:
        if not self.api_url or not self.api_key:
            return []
        self.log(f"Searching Usenet for '{author} {title}'...")

        url = f"{self.api_url}/api"
        # Newznab standard is an unquoted, space-separated query. Literal quotes around the phrase
        # cause many indexers (incl. most of Prowlarr's passthroughs) to treat it as an exact match
        # and return nothing — or to respond with an error page.
        params = {
            "t": "search",
            "cat": "7000,7020,8010",
            "q": f"{author} {title}",
            "apikey": self.api_key,
        }
        headers = {
            "User-Agent": UA,
            # Be explicit about what we expect. Some reverse proxies in front of Prowlarr fall back
            # to HTML (login / error pages) when Accept is */* — being explicit avoids that.
            "Accept": "application/rss+xml, application/xml, text/xml, application/json;q=0.9, */*;q=0.1",
        }

        try:
            async with httpx.AsyncClient(timeout=15.0, verify=False, follow_redirects=True, headers=headers) as client:
                resp = await client.get(url, params=params)

                ctype = resp.headers.get("Content-Type", "").lower()
                sent_url = str(resp.request.url)
                # Redact apikey in logged URLs so it doesn't leak into shared logs.
                safe_sent = re.sub(r'(apikey=)[^&]+', r'\1***', sent_url)
                safe_final = re.sub(r'(apikey=)[^&]+', r'\1***', str(resp.url))

                self.log(f"Usenet request: {safe_sent}")
                if resp.history:
                    self.log(f"Followed {len(resp.history)} redirect(s); final URL: {safe_final}")
                    for h in resp.history:
                        self.log(f"  {h.status_code} -> {h.headers.get('location', '?')}")
                self.log(f"Usenet response: status={resp.status_code} content-type='{ctype}' bytes={len(resp.content)}")

                if resp.status_code != 200:
                    self.log(f"Usenet API error: HTTP {resp.status_code}")
                    self.log(f"Body (first 500 chars): {resp.text[:500]}")
                    return []

                body = resp.text
                stripped = body.lstrip()
                looks_html = "text/html" in ctype or stripped[:15].lower().startswith(("<!doctype", "<html"))

                if looks_html:
                    self.log("ERROR: Prowlarr returned HTML instead of an RSS/XML or JSON feed.")
                    self.log("  Likely causes, in order of likelihood:")
                    self.log("   1. A reverse proxy / auth gateway in front of Prowlarr is intercepting the request.")
                    self.log("      If Prowlarr sits behind e.g. Authelia / nginx basic-auth, this container has no session cookie.")
                    self.log("   2. The Newznab URL is wrong. It should point to the indexer root, not the Prowlarr UI.")
                    self.log("      Correct form: http://<host>:9696/<indexer-id>  (the /api suffix is appended automatically)")
                    self.log("   3. Prowlarr's 'Authentication Required' is set to 'Enabled' and the apikey is being rejected.")
                    self.log(f"  Body (first 500 chars): {body[:500]}")
                    return []

                items = self._parse(body, ctype)
                self.log(f"Usenet parse found {len(items)} items")

                return self._match(items, author, title)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log(f"Usenet search error: {e}")
            return []

    def _parse(self, body: str, ctype: str) -> List[Dict]:
        # Prefer the parser that matches the content type, fall back to the other.
        if "json" in ctype:
            items = self._parse_json(body)
            return items if items else self._parse_xml(body)
        items = self._parse_xml(body)
        return items if items else self._parse_json(body)

    def _parse_xml(self, body: str) -> List[Dict]:
        items: List[Dict] = []
        try:
            # Drop the default xmlns so ET.find('item') works without namespace gymnastics.
            cleaned = re.sub(r'\sxmlns="[^"]+"', '', body, count=1)
            root = ET.fromstring(cleaned)
            # Newznab errors look like <error code="100" description="..." />
            if root.tag.lower() == 'error':
                self.log(f"Newznab error response: code={root.attrib.get('code')} desc={root.attrib.get('description')}")
                return []
            for item in root.iter('item'):
                t = item.findtext('title', default='') or ''
                l = item.findtext('link', default='') or ''
                enc = item.find('enclosure')
                enc_url = enc.attrib.get('url') if enc is not None else None
                items.append({"title": t.strip(), "link": l.strip(), "enclosure": enc_url})
        except ET.ParseError as e:
            self.log(f"XML parse error: {e}; falling back to regex extraction")
            for block in re.findall(r'<item[^>]*>(.*?)</item>', body, re.I | re.S):
                t = re.search(r'<title[^>]*>\s*(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?\s*</title>', block, re.I | re.S)
                l = re.search(r'<link[^>]*>\s*(.*?)\s*</link>', block, re.I | re.S)
                e_url = re.search(r'<enclosure[^>]+url=["\'](.*?)["\']', block, re.I | re.S)
                items.append({
                    "title": (t.group(1).strip() if t else ''),
                    "link": (l.group(1).strip() if l else ''),
                    "enclosure": (e_url.group(1).strip() if e_url else None),
                })
        return items

    def _parse_json(self, body: str) -> List[Dict]:
        try:
            data = json.loads(body)
        except Exception:
            return []
        raw = data.get("item") or data.get("channel", {}).get("item", [])
        if not isinstance(raw, list):
            raw = [raw] if raw else []
        items: List[Dict] = []
        for i in raw:
            enc = i.get("enclosure")
            enc_url = None
            if isinstance(enc, dict):
                enc_url = enc.get("@url") or enc.get("url")
            elif isinstance(enc, list):
                for e in enc:
                    if isinstance(e, dict):
                        enc_url = e.get("@url") or e.get("url")
                        if enc_url:
                            break
            items.append({"title": i.get("title", ""), "link": i.get("link", ""), "enclosure": enc_url})
        return items

    def _match(self, items: List[Dict], author: str, title: str) -> List[Dict]:
        results = []
        norm_title = normalize_text(title)
        author_parts = [p for p in normalize_text(author).split() if len(p) > 2]
        for item in items:
            res_title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', item.get("title", ""))
            norm_res_title = normalize_text(res_title)
            title_match = norm_title in norm_res_title
            author_match = any(p in norm_res_title for p in author_parts) if author_parts else True
            if not (title_match and author_match):
                continue
            link = item.get("enclosure") or item.get("link") or ""
            link = link.replace("&amp;", "&")
            if link:
                self.log(f"Found Usenet match: {res_title[:50]}...")
                results.append({"title": res_title, "link": link})
        return results

class SabnzbdClient:
    def __init__(self, url: str, api_key: str, log_func: Callable):
        self.url = url.strip().rstrip('/')
        self.api_key = api_key.strip()
        self.log = log_func

    async def add_url(self, nzb_url: str, title: str) -> Optional[str]:
        if not self.url or not self.api_key: return None
        params = {
            "mode": "addurl",
            "name": nzb_url,
            "nzbname": title,
            "cat": "books",
            "apikey": self.api_key,
            "output": "json"
        }
        try:
            async with httpx.AsyncClient(timeout=15.0, verify=False, follow_redirects=True, headers={"User-Agent": UA}) as client:
                resp = await client.get(f"{self.url}/api", params=params)
                data = resp.json()
                if data.get("status") and data.get("nzo_ids"):
                    nzo_id = data["nzo_ids"][0]
                    self.log(f"Sent to SABnzbd. ID: {nzo_id}")
                    return nzo_id
                self.log(f"SABnzbd add failed: {data}")
        except asyncio.CancelledError: raise
        except Exception as e:
            self.log(f"SABnzbd error: {e}")
        return None

    async def check_status(self, nzo_id: str) -> str:
        """Returns 'downloading', 'completed', 'failed', or 'unknown'"""
        try:
            async with httpx.AsyncClient(timeout=10.0, verify=False, follow_redirects=True, headers={"User-Agent": UA}) as client:
                # 1. Check Queue
                resp = await client.get(f"{self.url}/api", params={"mode": "queue", "nzo_id": nzo_id, "apikey": self.api_key, "output": "json"})
                q_data = resp.json()
                slots = q_data.get("queue", {}).get("slots", [])
                for s in slots:
                    if s.get("nzo_id") == nzo_id: return "downloading"
                
                # 2. Check History
                resp = await client.get(f"{self.url}/api", params={"mode": "history", "nzo_id": nzo_id, "apikey": self.api_key, "output": "json"})
                h_data = resp.json()
                slots = h_data.get("history", {}).get("slots", [])
                for s in slots:
                    if s.get("nzo_id") == nzo_id:
                        status = s.get("status", "").lower()
                        if status == "completed": return "completed"
                        if "failed" in status: return "failed"
        except asyncio.CancelledError: raise
        except Exception as e:
            self.log(f"SABnzbd status check error: {e}")
        return "unknown"

class MetadataFetcher:
    def __init__(self):
        self.client = httpx.Client(timeout=20.0, verify=False, headers={"User-Agent": UA})

    def search_author(self, name: str) -> List[Dict]:
        try:
            clean_name = name.replace(".", " ").strip().lower()
            response = self.client.get(f"https://openlibrary.org/search/authors.json", params={"q": clean_name})
            docs = response.json().get("docs", [])
            if not docs: return []
            candidates = sorted(docs[:10], key=lambda x: x.get("work_count", 0), reverse=True)[:5]
            detailed_results = []
            for doc in candidates:
                key = doc["key"]
                author_data = {"id": key, "name": doc.get("name"), "top_work": doc.get("top_work"), "work_count": doc.get("work_count"), "birth_date": doc.get("birth_date"), "bio": "", "photo_url": ""}
                try:
                    resp = self.client.get(f"https://openlibrary.org/authors/{key}.json")
                    if resp.status_code == 200:
                        data = resp.json()
                        bio = data.get("bio", "")
                        author_data["bio"] = bio.get("value", bio) if isinstance(bio, dict) else bio
                        if data.get("photos"): author_data["photo_url"] = f"https://covers.openlibrary.org/a/id/{data['photos'][0]}-M.jpg"
                except: pass
                detailed_results.append(author_data)
            return detailed_results
        except: return []

    def get_author_books(self, author_id: str, query: Optional[str] = None) -> List[Dict]:
        books, seen, page = [], set(), 1
        CRUFT = [r"\bbox set\b", r"\btrilogy\b", r"\bomnibus\b", r"\bselections\b", r"\bsummary\b", r"\banalysis\b", r"\bstudy guide\b", r"\bcompanion\b", r"\btrivia\b", r"\bunofficial\b", r"\bnotebook\b", r"\bdiary\b", r"\bjournal\b", r"\bcalendar\b", r"\bcatalog\b", r"\bbin\b", r"\bpack\b", r"\bx\d+\b", r"\bd/b\b", r"\bÚ10\b", r"\bvol\.\s*\d+\b", r"\bvolume\s*\d+\b", r"\bissue\b", r"\bmagazine\b", r"\breader\b", r"\bcollege\b", r"\bcourse\b", r"\btextbook\b", r"\bcookbook\b", r"\bkitchen\b", r"\bcomic\b", r"\bgraphic novel\b", r"\bmanga\b", r"\bedited by\b", r"\bvarious authors\b", r"\[\d+/\d+\]", r"\(part \d+\)", r"\bthe best (american|british|science|horror|mystery|sports)\b", r" / ", r" & ", r" ; "]
        while True:
            try:
                resp = self.client.get("https://openlibrary.org/search.json", params={"author": author_id, "language": "eng", "fields": "title,isbn,first_publish_year,subject", "page": page})
                docs = resp.json().get("docs", [])
                if not docs: break
                for doc in docs:
                    title = doc.get("title", "")
                    if not title or any(re.search(p, title.lower()) for p in CRUFT): continue
                    subjects = [s.lower() for s in doc.get("subject", [])]
                    NON_WANTED = ["history", "criticism", "manual", "biography", "non-fiction", "nonfiction", "bibliography", "cookbook", "calendar", "comics", "graphic novels", "periodicals", "study guide"]
                    WANTED_GENRES = ["fiction", "short stories", "novel", "literature", "science fiction", "fantasy", "speculative fiction", "cyberpunk", "horror", "mystery", "thriller"]
                    if subjects:
                        if any(term in s for term in NON_WANTED for s in subjects) and not any(term in s for term in WANTED_GENRES for s in subjects): continue
                    if subjects and not any(term in s for term in WANTED_GENRES for s in subjects): continue
                    norm = normalize_text(title)
                    if not norm or (query and normalize_text(query) not in norm): continue
                    if norm not in seen:
                        books.append({"title": title, "isbns": [i for i in doc.get("isbn", []) if len(i) in [10, 13]], "year": doc.get("first_publish_year")})
                        seen.add(norm)
                if len(docs) < 100: break
                page += 1
            except: break
        return sorted(books, key=lambda x: (x.get("year") or 0))

class ScraperEngine:
    def __init__(self, log_func: Callable):
        self.log = log_func
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

    async def _resolve_mirror(self, url: str, page: Browser) -> Optional[str]:
        try:
            await page.goto(url, timeout=15000)
            link = BeautifulSoup(await page.content(), 'html.parser').find('a', href=re.compile(r"get\.php|/get/|download", re.I))
            if link: return urljoin(url, link['href'])
        except asyncio.CancelledError: raise
        except Exception: pass
        return None

    async def get_mirrors(self, author: str, title: str, isbns: List[str]) -> List[Tuple[str, str]]:
        mirrors = []
        page = await self.browser.new_page()
        norm_title, author_parts = normalize_text(title), [p for p in normalize_text(author).split() if len(p) > 2]
        queries = [isbns[0]] if isbns else []
        clean_t = re.sub(r'^the\s+|^a\s+|^an\s+', '', title.lower())
        queries.append(f"{author} {clean_t}")
        try:
            for q in queries:
                self.log(f"Searching Libgen for '{q}'...")
                try:
                    await page.goto(f"https://libgen.li/index.php?req={quote(q)}&res=25&filesuns=all", timeout=30000)
                    soup = BeautifulSoup(await page.content(), 'html.parser')
                    rows = soup.select('table[id="table-libgen"] tr') or soup.find_all('tr')[1:]
                    for r in rows:
                        cols = r.find_all('td')
                        if len(cols) < 8: continue
                        raw_t, raw_a, raw_l, raw_e = cols[0].get_text(strip=True).lower(), cols[1].get_text(strip=True).lower(), cols[4].get_text(strip=True).lower(), cols[7].get_text(strip=True).lower()
                        if 'epub' in raw_e and (any(l in raw_l for l in ['english', 'eng']) or not raw_l.strip()) and norm_title in normalize_text(raw_t) and (any(p in raw_a for p in author_parts) if author_parts else True):
                            ads = cols[-1].find('a', href=re.compile(r"ads\.php"))
                            if ads:
                                direct = await self._resolve_mirror(urljoin("https://libgen.li", ads['href']), page)
                                if direct: self.log(f"Found Libgen match: {raw_t[:40]}..."); mirrors.append(("Libgen", direct)); break
                    if mirrors: break
                except asyncio.CancelledError: raise
                except Exception as e: self.log(f"Libgen error: {e}")

                self.log(f"Searching Anna's for '{q}'...")
                try:
                    await page.goto(f"{self.annas_base}/search?q={quote(q)}&ext=epub&lang=en", timeout=30000)
                    results = BeautifulSoup(await page.content(), 'html.parser').select('a[href*="/md5/"]')
                    for cand in results[:3]:
                        cand_t = normalize_text(cand.get_text())
                        if norm_title in cand_t and (any(p in cand_t for p in author_parts) if author_parts else True):
                            await page.goto(urljoin(self.annas_base, cand['href']), timeout=30000)
                            msoup = BeautifulSoup(await page.content(), 'html.parser')
                            lg = msoup.find('a', href=re.compile(r"libgen\.li/ads\.php"))
                            if lg:
                                direct = await self._resolve_mirror(lg['href'], page)
                                if direct: self.log(f"Found Anna match: {cand_t[:30]}..."); mirrors.append(("Anna Libgen", direct))
                            ipfs = msoup.find('a', href=re.compile(r"ipfs"))
                            if ipfs and 'ipfs://' in ipfs['href']: mirrors.append(("IPFS", f"https://ipfs.io/ipfs/{ipfs['href'].split('ipfs://')[1]}"))
                            if len(mirrors) >= 3: break
                    if mirrors: break
                except asyncio.CancelledError: raise
                except Exception as e: self.log(f"Anna error: {e}")
        finally: await page.close()
        return mirrors

class Downloader:
    def __init__(self, base_dir: str, log_func: Callable):
        self.base_dir = os.path.abspath(base_dir)
        self.log = log_func
        self.ssl_ctx = create_robust_ssl_context()
        os.makedirs(self.base_dir, exist_ok=True)
        try: os.chmod(self.base_dir, 0o777)
        except: pass

    async def download(self, mirror: str, url: str, author: str, title: str, book_data: Dict) -> bool:
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
                try: os.chmod(path, 0o777); os.chown(path, 65534, 65534)
                except Exception: pass
                if zipfile.is_zipfile(path):
                    with zipfile.ZipFile(path) as z:
                        if 'mimetype' in z.namelist():
                            try:
                                meta = ebookmeta.get_metadata(path)
                                meta.title, meta.author_list_to_string = title, author
                                ebookmeta.set_metadata(path, meta)
                                try: os.chmod(path, 0o777); os.chown(path, 65534, 65534)
                                except Exception: pass
                            except: pass
                            self.log(f"Saved to: {path}"); return True
                if os.path.exists(path): os.remove(path)
            except asyncio.CancelledError: raise
            except Exception:
                if os.path.exists(path): os.remove(path)
        return False
