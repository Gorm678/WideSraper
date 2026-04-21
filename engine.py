"""
engine.py — Core discovery engine for Citruss D365FO / Dynamics AX lead finder.
All configuration is read from config.json.
Feedback corrections are read from feedback.csv.
"""

import csv, os, re, json
from datetime import datetime
from urllib.parse import urlparse

# ── ERP patterns (hardcoded — these don't change) ─────────────────────────────
ERP_PATTERNS = [
    (r"\bd365fo\b",                                        "d365fo",               "D365FO"),
    (r"dynamics\s*365\s*finance\s*[&and]+\s*operations",   "dynamics 365 f&o",     "D365FO"),
    (r"dynamics\s*365\s*finance",                          "dynamics 365 finance", "D365FO"),
    (r"dynamics\s*ax",                                     "dynamics ax",          "Dynamics AX / Axapta"),
    (r"\baxapta\b",                                        "axapta",               "Dynamics AX / Axapta"),
    (r"\bax\s*2012\b",                                     "ax2012",               "Dynamics AX / Axapta"),
    (r"\bax\s*2009\b",                                     "ax2009",               "Dynamics AX / Axapta"),
    (r"\bax\s*4\.0\b",                                     "ax4.0",                "Dynamics AX / Axapta"),
    (r"\bax\s*3\.0\b",                                     "ax3.0",                "Dynamics AX / Axapta"),
    (r"\bax\s*2\.0\b",                                     "ax2.0",                "Dynamics AX / Axapta"),
    (r"\bx\+\+\b",                                         "x++",                  "X++ / AX Ecosystem"),
]
_COMPILED = [(re.compile(p, re.IGNORECASE), kw, erp) for p, kw, erp in ERP_PATTERNS]

EVIDENCE_RANK = {
    "direct_mention": 0, "procurement": 1, "partner_case": 2,
    "case_study": 3, "job_ad": 4, "web_mention": 5,
}

PARTNER_NAME_TOKENS = {
    "columbus", "fellowmind", "avanade", "cegeka", "norriq", "northvision",
    "logosconsult", "bredana", "axcite", "dynatech", "twentyfour", "addonax",
    "deloitte", "kpmg", "accenture", "capgemini", "pwc", "infosys", "wipro",
    "microsoft", "dynamics", "inetco", "abakion", "efacto", "tietoevry",
    "tieto", "evry", "bouvet", "soprasteria", "innofactor", "xperto",
    "affecto", "inmeta", "cgi", "sogeti", "synoptek", "concettolabs",
    "cosmonauts", "amcbanking", "avendata", "scales", "tbkconsult",
    "axsolutions", "paxa", "rand", "randgroup", "saglobal", "crayon",
}

PARTNER_PAGE_SIGNALS = [
    "microsoft partner", "gold partner", "dynamics partner",
    "certified partner", "implementation partner",
    "we implement", "we help companies implement",
    "we help organisations", "supplier of microsoft dynamics",
    "microsoft dynamics reseller", "vi implementerer",
]
DIRECT_ENDUSER_SIGNALS = [
    "our erp", "we use dynamics", "we run dynamics",
    "we implemented dynamics", "we went live on", "we went live with",
    "we migrated to", "we upgraded to", "we chose dynamics",
    "vores erp", "vi bruger dynamics", "vi implementerede dynamics",
    "vi gik live", "vi valgte dynamics",
    "vårt erp", "vi använder dynamics", "vi implementerade dynamics",
    "vi bruker dynamics", "vi implementerte dynamics",
]
CASE_STUDY_SIGNALS = [
    "case study", "customer story", "success story", "customer case",
    "how we helped", "we helped", "implemented for", "rolled out for",
    "go-live", "go live", "kundecase", "kundecasestudie",
    "kundhistoria", "kundhistorie", "vi hjalp", "vi hjälpte",
    "reference case", "customer reference", "deployed for",
]
JOB_PARTNER_SIGNALS = [
    "microsoft partner", "gold partner", "erp consulting firm",
    "consulting firm", "join our team of consultants",
    "client-facing", "client projects", "customer implementations",
    "you will implement", "we are a microsoft partner",
    "join a leading dynamics",
]
JOB_ENDUSER_SIGNALS = [
    "internal erp", "in-house erp", "our internal dynamics",
    "our finance team", "our it department", "our global erp",
    "group erp", "internt erp", "vores interne",
]

_LEGAL_SUFFIX = re.compile(
    r"[-_\s]?(a/?s|aps|i/?s|k/?s|ab|asa|nv|bv|gmbh|ltd|llc|plc|inc|"
    r"holding|group|gruppen|nordic|nord|global|international|online|digital|"
    r"denmark|danmark|sweden|sverige|norway|norge)$",
    re.IGNORECASE,
)
_GENERIC_APEX = re.compile(
    r"^(www|mail|portal|intranet|careers|jobs|hr|erp|crm|"
    r"shop|store|web|app|mysite|sharepoint|teams|blog|news)$",
    re.IGNORECASE,
)
_EXTRACTION_PATTERNS = [
    re.compile(
        r'^([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&\s]{2,40}?)\s+'
        r'(?:implements|implemented|upgrades|upgraded|deploys|deployed|'
        r'goes live|went live|chooses|chose|selects|selected|'
        r'implementerer|implementerede|vælger|valgte)',
        re.IGNORECASE,
    ),
    re.compile(
        r'\b(?:helping|helped|assisting|assisted|supporting|supported|'
        r'hjälpte|hjalp|hjelper)\s+'
        r'([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}(?:\s+[A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}){0,3})',
        re.IGNORECASE,
    ),
    re.compile(
        r'\bfor\s+([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}(?:\s+[A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}){0,2})',
        re.IGNORECASE,
    ),
    re.compile(
        r'\bat\s+([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}(?:\s+[A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}){0,2})',
        re.IGNORECASE,
    ),
    re.compile(
        r'\b(?:hos|til)\s+([A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}(?:\s+[A-ZÆØÅÄ][A-Za-zÆØÅÄæøåä0-9\-\.&]{2,}){0,2})',
        re.IGNORECASE,
    ),
]
_SKIP_IN_EXTRACTION = {
    "Denmark","Danish","Danmark","Dansk","Sweden","Swedish","Sverige","Svensk",
    "Norway","Norwegian","Norge","Norsk","Dynamics","Microsoft","Finance",
    "Operations","Supply","Chain","Axapta","Nordic","Europe","European",
    "Azure","Power","Business","Central","The","This","Our","We","They",
    "ERP","AX","D365","D365FO","SAP",
}
COUNTRY_TLD_MAP = {".dk": "Denmark", ".se": "Sweden", ".no": "Norway"}
_COUNTRY_SIGNALS = {
    "Denmark": ["denmark","danish","danmark","dansk","københavn","aarhus","odense"],
    "Sweden":  ["sweden","swedish","sverige","svensk","stockholm","göteborg","malmö"],
    "Norway":  ["norway","norwegian","norge","norsk","oslo","bergen","trondheim"],
}

AI_SYSTEM_PROMPT = """You are a B2B lead researcher identifying companies that use Microsoft Dynamics 365 Finance & Operations (D365FO) or legacy Dynamics AX / Axapta as their ERP system.

For each evidence row, decide:
1. Is the source owner a real end-user, or a partner/vendor/recruiter/content site?
2. If partner/vendor/recruiter: can you extract a named end-user company?
3. What is the best target_company name?

Rules:
- Partners, consultancies, ISVs, recruiters, job boards, training/media sites are NOT targets.
- Only real organizations that USE the ERP internally are targets.
- For case studies: extract the customer name.
- For job ads: extract hiring employer ONLY if they are an end-user (not a consulting firm).
- If no real end-user can be identified, set is_target to false.

Respond ONLY with a JSON array. Each element:
{
  "row_id": <integer>,
  "is_target": <true|false>,
  "target_company": "<name or empty string>",
  "classification": "<end_user_direct|end_user_referenced|partner_or_vendor|recruiter_or_job_board|content_or_training|noise>",
  "confidence": "<high|medium|low>",
  "reason": "<one sentence>"
}"""


class DiscoveryEngine:
    def __init__(self, config_path="config.json", feedback_path="feedback.csv"):
        self.config_path   = config_path
        self.feedback_path = feedback_path
        self.config        = self._load_config()
        self.feedback      = self._load_feedback()
        self._build_sets()

    # ── Config & feedback ─────────────────────────────────────────────────────

    def _load_config(self):
        with open(self.config_path, encoding="utf-8") as f:
            return json.load(f)

    def _load_feedback(self):
        """Load feedback.csv → dict of {domain_or_name: action}"""
        fb = {}
        if not os.path.exists(self.feedback_path):
            return fb
        with open(self.feedback_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key    = row.get("domain_or_name", "").strip().lower()
                action = row.get("action", "").strip().lower()
                if key and action:
                    fb[key] = action
        return fb

    def _build_sets(self):
        """Merge config blocklists with feedback corrections."""
        cfg = self.config
        self.noise_domains    = set(cfg.get("noise_domains", []))
        self.content_domains  = set(cfg.get("content_domains", []))
        self.job_board_domains= set(cfg.get("job_board_domains", []))
        self.recruiter_domains= set(cfg.get("recruiter_domains", []))
        self.training_domains = set(cfg.get("training_domains", []))
        self.partner_domains  = set(cfg.get("partner_domains", []))
        self.bad_targets      = set(n.lower() for n in cfg.get("bad_target_names", []))

        # Apply feedback: "block_partner", "block_noise", "block_job_board"
        for key, action in self.feedback.items():
            if action == "block_partner":   self.partner_domains.add(key)
            elif action == "block_noise":   self.noise_domains.add(key)
            elif action == "block_job_board": self.job_board_domains.add(key)
            elif action == "bad_name":      self.bad_targets.add(key)

    def save_feedback(self, domain_or_name, action, note=""):
        """Append one feedback row and rebuild sets."""
        exists = os.path.exists(self.feedback_path)
        with open(self.feedback_path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["domain_or_name","action","note","added_at"])
            if not exists:
                w.writeheader()
            w.writerow({
                "domain_or_name": domain_or_name.lower().strip(),
                "action":         action,
                "note":           note,
                "added_at":       datetime.now().strftime("%Y-%m-%d %H:%M"),
            })
        self.feedback = self._load_feedback()
        self._build_sets()

    # ── Domain classification ─────────────────────────────────────────────────

    def classify_source_owner(self, domain):
        def hit(s): return domain in s or any(domain.endswith("."+d) for d in s)
        if hit(self.noise_domains):     return "noise"
        if hit(self.content_domains):   return "content_or_training"
        if hit(self.training_domains):  return "content_or_training"
        if hit(self.job_board_domains): return "recruiter_or_job_board"
        if hit(self.recruiter_domains): return "recruiter_or_job_board"
        if hit(self.partner_domains):   return "partner_or_vendor"
        return "unknown"

    # ── Name helpers ──────────────────────────────────────────────────────────

    def name_from_domain(self, domain):
        apex = domain.split(".")[0].lower()
        if _GENERIC_APEX.match(apex): return ""
        apex = _LEGAL_SUFFIX.sub("", apex).strip("-_")
        if not apex or apex in PARTNER_NAME_TOKENS: return ""
        return " ".join(w.capitalize() for w in re.split(r"[-_]+", apex) if w)

    def is_bad_target(self, name):
        if not name or len(name.strip()) < 3: return True
        return name.strip().lower() in self.bad_targets

    def extract_target_company(self, title, snippet):
        for text in [title, snippet]:
            if not text: continue
            for pattern in _EXTRACTION_PATTERNS:
                for m in pattern.finditer(text):
                    candidate = re.sub(r'[,\.\s]+$', '', m.group(1).strip())
                    low = candidate.lower().split()[0] if candidate else ""
                    if candidate in _SKIP_IN_EXTRACTION: continue
                    if low in PARTNER_NAME_TOKENS:        continue
                    if self.is_bad_target(candidate):     continue
                    return candidate
        for text in [title, snippet]:
            if not text: continue
            for c in re.findall(r'\b([A-ZÆØÅÄ][a-zæøåä]+(?:\s[A-ZÆØÅÄ][a-zæøåä]+){0,2})\b', text):
                if c in _SKIP_IN_EXTRACTION:           continue
                if c.lower() in PARTNER_NAME_TOKENS:   continue
                if c.lower() in self.bad_targets:      continue
                if len(c) < 4:                         continue
                return c
        return ""

    # ── Row classification ────────────────────────────────────────────────────

    def classify_row(self, url, title, desc, source_owner_class):
        combined = f"{title} {desc}".lower()
        if source_owner_class == "noise":
            return "noise", "noise", ""
        if source_owner_class == "content_or_training":
            return "content_or_training", "content", ""
        if source_owner_class == "recruiter_or_job_board":
            if any(s in combined for s in JOB_PARTNER_SIGNALS):
                return "recruiter_or_job_board", "job_ad", ""
            target = self.extract_target_company(title, desc)
            if target:
                return "end_user_referenced", "job_ad", target
            return "recruiter_or_job_board", "job_ad", ""
        if source_owner_class == "partner_or_vendor":
            if any(s in combined for s in CASE_STUDY_SIGNALS):
                target = self.extract_target_company(title, desc)
                if target:
                    return "end_user_referenced", "partner_case", target
            return "partner_or_vendor", "partner_page", ""
        if any(s in combined for s in PARTNER_PAGE_SIGNALS):
            if any(s in combined for s in CASE_STUDY_SIGNALS):
                target = self.extract_target_company(title, desc)
                if target:
                    return "end_user_referenced", "partner_case", target
            return "partner_or_vendor", "partner_page", ""
        if any(s in combined for s in DIRECT_ENDUSER_SIGNALS):
            return "end_user_direct", "direct_mention", ""
        if any(s in combined for s in CASE_STUDY_SIGNALS):
            target = self.extract_target_company(title, desc)
            if target:
                return "end_user_referenced", "case_study", target
        if any(s in combined for s in JOB_ENDUSER_SIGNALS):
            return "end_user_direct", "job_ad", ""
        return "end_user_direct", "web_mention", ""

    def is_valid_target(self, classification, target_company):
        if classification == "end_user_direct":
            return True
        if classification == "end_user_referenced":
            return bool(target_company) and not self.is_bad_target(target_company)
        return False

    # ── Country detection ─────────────────────────────────────────────────────

    def detect_country(self, url, title, snippet, query_country):
        domain = self._extract_domain(url)
        for tld, country in COUNTRY_TLD_MAP.items():
            if domain.endswith(tld): return country
        text_lower = f"{url} {title} {snippet}".lower()
        scores = {c: sum(1 for s in sigs if s in text_lower)
                  for c, sigs in _COUNTRY_SIGNALS.items()}
        best = max(scores, key=scores.get)
        return best if scores[best] > 0 else query_country

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _extract_domain(self, url):
        try:
            return urlparse(url).netloc.lower().lstrip("www.")
        except:
            return ""

    def _safe(self, obj, attr):
        return (obj.get(attr,"") if isinstance(obj,dict) else getattr(obj,attr,"")) or ""

    def _find_matches(self, text):
        seen, out = set(), []
        for pat, kw, erp in _COMPILED:
            if kw not in seen and pat.search(text):
                seen.add(kw); out.append((pat, kw, erp))
        return out

    def _get_snippet(self, text, pat, window=300):
        m = pat.search(text)
        if not m: return ""
        s, e = max(0, m.start()-window), min(len(text), m.end()+window)
        return " ".join(text[s:e].split())

    # ── AI enrichment ─────────────────────────────────────────────────────────

    def should_send_to_ai(self, row):
        cls, etype = row["classification"], row["evidence_type"]
        if cls in ("noise", "content_or_training"): return False
        if cls == "partner_or_vendor": return False
        if cls == "end_user_direct" and etype == "direct_mention": return False
        if cls == "end_user_direct" and etype == "web_mention": return True
        if cls == "end_user_referenced" and etype in ("job_ad", "partner_case"): return True
        return False

    def ai_enrich(self, rows_to_review, progress_cb=None):
        if not rows_to_review: return {}
        import urllib.request

        provider = os.environ.get("AI_PROVIDER", "openai").lower()
        if provider == "anthropic":
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if not api_key:
                if progress_cb: progress_cb("⚠ ANTHROPIC_API_KEY not set — skipping AI")
                return {}
            url     = "https://api.anthropic.com/v1/messages"
            headers = {
                "Content-Type": "application/json",
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            }
            def make_payload(msgs):
                return json.dumps({
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 1000,
                    "system": AI_SYSTEM_PROMPT,
                    "messages": msgs,
                }).encode("utf-8")
        else:  # openai default
            api_key = os.environ.get("OPENAI_API_KEY", "")
            if not api_key:
                if progress_cb: progress_cb("⚠ OPENAI_API_KEY not set — skipping AI")
                return {}
            url     = "https://api.openai.com/v1/chat/completions"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            }
            def make_payload(msgs):
                return json.dumps({
                    "model": "gpt-4o",
                    "temperature": 0,
                    "messages": [
                        {"role": "system", "content": AI_SYSTEM_PROMPT},
                    ] + msgs,
                }).encode("utf-8")

        batch_size = self.config.get("ai_batch_size", 20)
        all_results = {}
        total_batches = (len(rows_to_review) - 1) // batch_size + 1

        for i in range(0, len(rows_to_review), batch_size):
            batch = rows_to_review[i:i+batch_size]
            batch_num = i // batch_size + 1
            if progress_cb:
                progress_cb(f"AI batch {batch_num}/{total_batches} ({len(batch)} rows)...")

            items = [{
                "row_id":           r["_row_id"],
                "source_domain":    r["source_domain"],
                "evidence_title":   r["evidence_title"],
                "evidence_snippet": r["evidence_snippet"][:400],
                "erp_detected":     r["erp_detected"],
                "rule_classification": r["classification"],
                "rule_target":      r["target_company"],
            } for r in batch]

            prompt_text = json.dumps(items, ensure_ascii=False, indent=2)
            msgs = [{"role": "user", "content": prompt_text}]
            payload = make_payload(msgs)
            req = urllib.request.Request(url, data=payload, headers=headers, method="POST")

            try:
                with urllib.request.urlopen(req, timeout=45) as resp:
                    data = json.loads(resp.read())
                if provider == "anthropic":
                    text = data["content"][0]["text"].strip()
                else:
                    text = data["choices"][0]["message"]["content"].strip()
                text = re.sub(r'^```json\s*', '', text)
                text = re.sub(r'\s*```$',    '', text)
                results = json.loads(text)
                for r in results:
                    all_results[r["row_id"]] = r
            except Exception as e:
                if progress_cb: progress_cb(f"⚠ AI error on batch {batch_num}: {e}")

        return all_results

    def apply_ai_results(self, rows, ai_results):
        for r in rows:
            rid = r.get("_row_id")
            if rid not in ai_results: continue
            ai = ai_results[rid]
            r["classification"] = ai.get("classification", r["classification"])
            r["ai_confidence"]  = ai.get("confidence", "")
            r["ai_reason"]      = ai.get("reason", "")
            if not ai.get("is_target"):
                r["is_target"] = False
            else:
                if ai.get("target_company") and not self.is_bad_target(ai["target_company"]):
                    r["target_company"] = ai["target_company"]
                r["is_target"] = True
        return rows

    # ── Main search ───────────────────────────────────────────────────────────

    def run(self, countries=None, progress_cb=None):
        from firecrawl import FirecrawlApp

        api_key = os.environ.get("FIRECRAWL_API_KEY", "")
        if not api_key:
            raise ValueError("FIRECRAWL_API_KEY not set")

        app     = FirecrawlApp(api_key=api_key)
        queries = self.config.get("queries", {})
        limit   = self.config.get("result_limit", 10)
        use_ai  = self.config.get("use_ai_enrichment", True)

        if countries:
            queries = {c: q for c, q in queries.items() if c in countries}

        rows   = []
        row_id = 0

        for country, query_list in queries.items():
            if progress_cb: progress_cb(f"── {country} ({len(query_list)} queries)")
            for i, query in enumerate(query_list, 1):
                if progress_cb: progress_cb(f"  [{i:02d}/{len(query_list)}] {query}")
                try:
                    results = app.search(query, limit=limit)
                except Exception as e:
                    if progress_cb: progress_cb(f"    ✗ {e}")
                    continue

                items = (getattr(results,"web",None)
                         or getattr(results,"data",None)
                         or (results if isinstance(results,list) else []))

                hits = 0
                for item in items:
                    title  = self._safe(item, "title")
                    url    = self._safe(item, "url")
                    desc   = self._safe(item, "description")
                    domain = self._extract_domain(url)
                    text   = f"{title}\n{desc}"

                    matches = self._find_matches(text)
                    if not matches: continue

                    src_class    = self.classify_source_owner(domain)
                    source_owner = self.name_from_domain(domain) or domain
                    cls, etype, target = self.classify_row(url, title, desc, src_class)

                    if cls == "end_user_direct" and not target:
                        target = self.name_from_domain(domain)
                    target_domain  = domain if cls == "end_user_direct" else ""
                    country_det    = self.detect_country(url, title, desc, country)
                    valid          = self.is_valid_target(cls, target)

                    for pat, kw, erp in matches:
                        row_id += 1
                        rows.append({
                            "_row_id":          row_id,
                            "target_company":   target,
                            "target_domain":    target_domain,
                            "country":          country_det,
                            "erp_detected":     erp,
                            "matched_keyword":  kw,
                            "classification":   cls,
                            "evidence_type":    etype,
                            "evidence_url":     url,
                            "evidence_title":   title,
                            "evidence_snippet": self._get_snippet(text, pat),
                            "source_owner":     source_owner,
                            "source_domain":    domain,
                            "query":            query,
                            "query_country":    country,
                            "is_target":        valid,
                            "ai_reason":        "",
                            "ai_confidence":    "",
                        })
                        hits += 1

                if progress_cb: progress_cb(f"    ✓ {hits} hits from {len(items)} results")

        # AI enrichment
        if use_ai and rows:
            ambiguous = [r for r in rows if self.should_send_to_ai(r)]
            if progress_cb: progress_cb(f"\n── AI review: {len(ambiguous)} ambiguous rows")
            if ambiguous:
                ai_results = self.ai_enrich(ambiguous, progress_cb=progress_cb)
                rows = self.apply_ai_results(rows, ai_results)

        return rows

    # ── Build deduplicated target list ────────────────────────────────────────

    def build_targets(self, rows):
        seen = {}
        for r in rows:
            if not r["is_target"]: continue
            name = r["target_company"].strip()
            if self.is_bad_target(name): continue
            if name.lower().split()[0] in PARTNER_NAME_TOKENS: continue
            key = f"{name.lower()}|{r['country']}"
            if key not in seen:
                seen[key] = r
            else:
                cur = EVIDENCE_RANK.get(seen[key]["evidence_type"], 99)
                new = EVIDENCE_RANK.get(r["evidence_type"], 99)
                if new < cur: seen[key] = r
                elif new == cur and len(r["evidence_snippet"]) > len(seen[key]["evidence_snippet"]):
                    seen[key] = r
        return sorted(seen.values(), key=lambda r: (r["country"], r["target_company"].lower()))

    # ── Save CSVs + merge into master list ───────────────────────────────────

    def save_results(self, rows, output_dir="outputs"):
        from masterlist import MasterList
        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M")

        raw_path = os.path.join(output_dir, f"raw_{ts}.csv")
        raw_fields = [
            "target_company","target_domain","country","erp_detected","matched_keyword",
            "classification","evidence_type","evidence_url","evidence_title",
            "evidence_snippet","source_owner","source_domain",
            "query","query_country","is_target","ai_reason","ai_confidence",
        ]
        with open(raw_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=raw_fields, extrasaction="ignore")
            w.writeheader(); w.writerows(rows)

        targets     = self.build_targets(rows)
        target_path = os.path.join(output_dir, f"targets_{ts}.csv")
        target_fields = [
            "target_company","target_domain","country","erp_detected",
            "classification","evidence_type","evidence_url","evidence_title",
            "evidence_snippet","source_owner","ai_reason","ai_confidence",
        ]
        with open(target_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=target_fields, extrasaction="ignore")
            w.writeheader(); w.writerows(targets)

        # Auto-merge into master list
        master = MasterList()
        counts = master.merge_run(targets)

        return raw_path, target_path, targets, counts
