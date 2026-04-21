"""
scraper.py — Partner case study scraper for Citruss ERP Discovery.

Uses Firecrawl to:
1. Crawl partner case study index pages
2. Find links to individual case studies
3. Scrape each case study for customer name, ERP, industry, snippet

Plugs into engine.py and app.py — results flow into the same targets CSV.
"""

import re, json, os
from urllib.parse import urlparse, urljoin


# ── ERP detection (same as engine.py) ────────────────────────────────────────
ERP_PATTERNS = [
    (r"\bd365fo\b",                                        "d365fo",               "D365FO"),
    (r"dynamics\s*365\s*finance\s*[&and]+\s*operations",   "dynamics 365 f&o",     "D365FO"),
    (r"dynamics\s*365\s*finance",                          "dynamics 365 finance", "D365FO"),
    (r"dynamics\s*ax",                                     "dynamics ax",          "Dynamics AX / Axapta"),
    (r"\baxapta\b",                                        "axapta",               "Dynamics AX / Axapta"),
    (r"\bax\s*2012\b",                                     "ax2012",               "Dynamics AX / Axapta"),
    (r"\bax\s*2009\b",                                     "ax2009",               "Dynamics AX / Axapta"),
    (r"\bx\+\+\b",                                         "x++",                  "X++ / AX Ecosystem"),
]
_COMPILED = [(re.compile(p, re.IGNORECASE), kw, erp) for p, kw, erp in ERP_PATTERNS]

COUNTRY_SIGNALS = {
    "Denmark": ["denmark","danish","danmark","dansk","københavn","aarhus","odense","aalborg"],
    "Sweden":  ["sweden","swedish","sverige","svensk","stockholm","göteborg","malmö","linköping"],
    "Norway":  ["norway","norwegian","norge","norsk","oslo","bergen","trondheim","stavanger"],
}

# Words that appear in case study titles but aren't company names
_BAD_NAMES = {
    "case", "study", "customer", "story", "success", "how", "why", "what",
    "the", "our", "their", "this", "with", "for", "and", "but", "read",
    "more", "view", "see", "watch", "learn", "download", "contact",
    "dynamics", "microsoft", "axapta", "erp", "finance", "operations",
    "implementation", "migration", "upgrade", "rollout", "digital",
    "transformation", "solution", "project", "reference", "kunde",
    "kund", "kundecase", "kundcase", "referanse", "referens",
    "denmark", "sweden", "norway", "nordic", "global",
}

_PARTNER_NAME_TOKENS = {
    "columbus", "fellowmind", "cegeka", "norriq", "abakion",
    "elbek", "vejrup", "be-terna", "beterna", "innofactor",
    "xperto", "bouvet", "soprasteria", "tietoevry",
    "microsoft", "dynamics",
}

# Patterns to extract company from case study title/heading
_TITLE_EXTRACT = [
    # "Vestas chose Dynamics 365" / "How Vestas implemented..."
    re.compile(
        r'^(?:how\s+)?([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&\s]{1,40}?)\s+'
        r'(?:chose|selected|implemented|deployed|upgraded|migrated|went live|'
        r'valgte|implementerede|opgraderede|valde|implementerade|valgte|implementerte)',
        re.IGNORECASE,
    ),
    # "Case study: Vestas" / "Kunde: Vestas" / "Reference: Vestas"
    re.compile(
        r'(?:case study|customer story|success story|kunde|kund|kundecase|'
        r'kundcase|referanse|referens|reference|client)\s*[:\-–]\s*'
        r'([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&\s]{2,50})',
        re.IGNORECASE,
    ),
    # "Vestas | Columbus" — company name before partner name in title
    re.compile(
        r'^([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&\s]{2,40}?)\s*[|\-–]\s*',
        re.IGNORECASE,
    ),
]

_SKIP_IN_TITLE = {
    "How", "Why", "What", "When", "Read", "See", "View", "Download",
    "Case", "Customer", "Success", "Reference", "Kunde", "Kund",
    "Denmark", "Sweden", "Norway", "Nordic", "Global", "Microsoft",
    "Dynamics", "Finance", "Operations", "The", "Our", "Their",
}


def extract_domain(url):
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except:
        return ""


def detect_erp(text):
    """Return list of (keyword, erp_bucket) matches."""
    seen, out = set(), []
    for pat, kw, erp in _COMPILED:
        if kw not in seen and pat.search(text):
            seen.add(kw)
            out.append((kw, erp))
    return out


def detect_country(url, text, partner_country):
    """Detect country from TLD, then text signals, then fall back to partner default."""
    domain = extract_domain(url)
    tld_map = {".dk": "Denmark", ".se": "Sweden", ".no": "Norway"}
    for tld, country in tld_map.items():
        if domain.endswith(tld):
            return country
    text_lower = text.lower()
    scores = {c: sum(1 for s in sigs if s in text_lower)
              for c, sigs in COUNTRY_SIGNALS.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else partner_country


def get_snippet(text, window=300):
    """Return snippet around first ERP keyword match."""
    for pat, _, _ in _COMPILED:
        m = pat.search(text)
        if m:
            s = max(0, m.start() - window)
            e = min(len(text), m.end() + window)
            return " ".join(text[s:e].split())
    return text[:500].strip()


def extract_company_from_title(title):
    """Try to extract company name from case study page title."""
    if not title:
        return ""

    # Try structured patterns first
    for pattern in _TITLE_EXTRACT:
        m = pattern.search(title.strip())
        if m:
            candidate = m.group(1).strip()
            candidate = re.sub(r'[,\.\s]+$', '', candidate)
            words = candidate.lower().split()
            if not words:
                continue
            if candidate in _SKIP_IN_TITLE:
                continue
            if words[0] in _PARTNER_NAME_TOKENS:
                continue
            if words[0] in _BAD_NAMES:
                continue
            if len(candidate) < 3:
                continue
            return candidate

    # Fallback: first capitalised segment before a separator
    for seg in re.split(r'\s*[|\-–—:·]\s*', title):
        seg = seg.strip()
        if len(seg) < 3:
            continue
        if seg.lower() in _BAD_NAMES:
            continue
        if seg.lower().split()[0] in _PARTNER_NAME_TOKENS:
            continue
        if seg[0].isupper():
            return seg

    return ""


def extract_company_from_content(text):
    """
    Scan full page content for company name near ERP keywords.
    Uses structured patterns then falls back to proper noun scan.
    """
    _CONTENT_PATTERNS = [
        re.compile(
            r'\b([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}(?:\s+[A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}){0,3})\s+'
            r'(?:implemented|deployed|chose|selected|upgraded|migrated|went live|'
            r'uses|is using|has implemented|has deployed|'
            r'implementerede|valgte|opgraderede|bruger|'
            r'implementerade|valde|uppgraderade|använder|'
            r'implementerte|valgte|oppgraderte|bruker)',
            re.IGNORECASE,
        ),
        re.compile(
            r'(?:helped|helping|assisted|assisting|supporting|supported|'
            r'hjalp|hjälpte)\s+'
            r'([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}(?:\s+[A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}){0,3})',
            re.IGNORECASE,
        ),
        re.compile(
            r'(?:customer|client|kunde|kund|kundecase)\s*[:\-]?\s*'
            r'([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}(?:\s+[A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}){0,2})',
            re.IGNORECASE,
        ),
    ]

    skip = {
        "Denmark", "Danish", "Danmark", "Sweden", "Swedish", "Sverige",
        "Norway", "Norwegian", "Norge", "Dynamics", "Microsoft", "Finance",
        "Operations", "Axapta", "Nordic", "Europe", "Azure", "The", "This",
        "Our", "We", "They", "ERP", "AX", "D365", "D365FO",
    }

    for pattern in _CONTENT_PATTERNS:
        for m in pattern.finditer(text[:5000]):  # scan first 5000 chars
            candidate = re.sub(r'[,\.\s]+$', '', m.group(1).strip())
            if candidate in skip:
                continue
            if candidate.lower().split()[0] in _PARTNER_NAME_TOKENS:
                continue
            if candidate.lower() in _BAD_NAMES:
                continue
            if len(candidate) < 3:
                continue
            return candidate

    return ""


class CaseStudyScraper:

    def __init__(self, firecrawl_app, partner_sites, progress_cb=None):
        self.app         = firecrawl_app
        self.partners    = partner_sites  # list of dicts from config
        self.progress_cb = progress_cb or (lambda msg: None)

    def log(self, msg):
        self.progress_cb(msg)

    def scrape_all(self):
        """
        Main entry point. Returns list of result dicts compatible with
        engine.py row format.
        """
        all_rows = []
        for partner in self.partners:
            name         = partner["name"]
            index_url    = partner["index_url"]
            country      = partner["country"]
            max_cases    = partner.get("max_cases", 30)

            self.log(f"\n── {name} ({country})")
            self.log(f"  Index: {index_url}")

            # Step 1: get case study links from index page
            links = self._get_case_links(index_url, max_cases)
            self.log(f"  Found {len(links)} case study links")

            if not links:
                continue

            # Step 2: scrape each case study
            for i, link in enumerate(links, 1):
                self.log(f"  [{i:02d}/{len(links)}] {link}")
                rows = self._scrape_case(link, name, country)
                if rows:
                    all_rows.extend(rows)
                    self.log(f"    ✓ {rows[0]['target_company']} — {rows[0]['erp_detected']}")
                else:
                    self.log(f"    – no ERP signal found")

        self.log(f"\n── Case study scraping complete: {len(all_rows)} rows")
        return all_rows

    def _get_case_links(self, index_url, max_cases):
        """Scrape the index page and extract links to individual case studies."""
        try:
            result = self.app.scrape(
                index_url,
                formats=["links", "markdown"],
            )
        except Exception as e:
            self.log(f"  ✗ Failed to scrape index: {e}")
            return []

        # Get links from result
        links = []
        raw_links = []

        if hasattr(result, 'links') and result.links:
            raw_links = result.links
        elif isinstance(result, dict) and result.get('links'):
            raw_links = result['links']

        base_domain = extract_domain(index_url)
        base_path   = urlparse(index_url).path.rstrip('/')

        for link in raw_links:
            # Handle both string links and dict links
            href = link if isinstance(link, str) else link.get('url', '')
            if not href:
                continue

            # Must be on same domain
            link_domain = extract_domain(href)
            if link_domain and link_domain != base_domain:
                continue

            # Must look like a case study URL
            href_lower = href.lower()
            case_indicators = [
                '/case', '/kunde', '/kund', '/referans', '/referenc',
                '/success', '/customer', '/story', '/project', '/referencen',
                '/cases/', '/kunder/', '/referencer/', '/referanser/',
            ]
            if not any(ind in href_lower for ind in case_indicators):
                continue

            # Skip index page itself and generic pages
            skip_paths = ['#', 'javascript:', 'mailto:', '/tag/', '/category/',
                         '/filter/', '/search/', '/page/', '?']
            if any(s in href for s in skip_paths):
                continue

            # Must be deeper than index URL
            link_path = urlparse(href).path
            if link_path.rstrip('/') == base_path:
                continue

            if href not in links:
                links.append(href)

            if len(links) >= max_cases:
                break

        return links

    def _scrape_case(self, url, partner_name, partner_country):
        """Scrape one case study page. Returns list of row dicts or empty list."""
        try:
            result = self.app.scrape(url, formats=["markdown"])
        except Exception as e:
            self.log(f"    ✗ Scrape failed: {e}")
            return []

        # Extract content
        content = ""
        title   = ""
        if hasattr(result, 'markdown') and result.markdown:
            content = result.markdown
        elif isinstance(result, dict):
            content = result.get('markdown', '') or result.get('content', '')

        if hasattr(result, 'metadata') and result.metadata:
            title = getattr(result.metadata, 'title', '') or ''
            if isinstance(result.metadata, dict):
                title = result.metadata.get('title', '')
        elif isinstance(result, dict) and result.get('metadata'):
            title = result['metadata'].get('title', '')

        if not content and not title:
            return []

        full_text = f"{title}\n{content}"

        # Check for ERP keywords
        erp_matches = detect_erp(full_text)
        if not erp_matches:
            return []

        # Try to extract company name
        company = extract_company_from_title(title)
        if not company:
            company = extract_company_from_content(content)
        if not company:
            return []

        # Detect country
        country = detect_country(url, full_text, partner_country)

        # Build snippet
        snippet = get_snippet(full_text)

        # One row per unique ERP bucket detected
        seen_erp, rows = set(), []
        for kw, erp in erp_matches:
            if erp in seen_erp:
                continue
            seen_erp.add(erp)
            rows.append({
                "_row_id":          f"cs_{hash(url+erp)}",
                "target_company":   company,
                "target_domain":    "",
                "country":          country,
                "erp_detected":     erp,
                "matched_keyword":  kw,
                "classification":   "end_user_referenced",
                "evidence_type":    "partner_case_scraped",
                "evidence_url":     url,
                "evidence_title":   title[:200] if title else url,
                "evidence_snippet": snippet[:600],
                "source_owner":     partner_name,
                "source_domain":    extract_domain(url),
                "query":            f"scrape:{partner_name}",
                "query_country":    partner_country,
                "is_target":        True,
                "ai_reason":        f"Extracted from {partner_name} case study page",
                "ai_confidence":    "high",
            })

        return rows
