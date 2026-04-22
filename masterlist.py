"""
masterlist.py — Persistent master list of all discovered companies.

Tracks every company ever found, when it was first seen, how many times
it has appeared, and whether it has been actioned by a sales rep.

The master list is the single source of truth for:
- Which companies are net-new (never seen before)
- Which companies have been seen before (and when)
- Which companies have been actioned (added to CRM, dismissed, etc.)
"""

import csv, os
from datetime import datetime

MASTER_PATH   = "master_list.csv"
MASTER_FIELDS = [
    "company_key",       # lowercase name|country — dedup key
    "company_name",
    "country",
    "erp_detected",
    "first_seen",        # date of first discovery
    "last_seen",         # date of most recent discovery
    "times_seen",        # how many runs this company has appeared in
    "best_evidence_type",
    "best_evidence_url",
    "best_evidence_title",
    "best_snippet",
    "source_owner",
    "status",            # new | seen | actioned_crm | actioned_dismissed
    "status_note",       # rep's note when actioning
    "status_date",       # when status was last changed
]

EVIDENCE_RANK = {
    "direct_mention":       0,
    "partner_case_scraped": 1,
    "procurement":          2,
    "partner_case":         3,
    "case_study":           4,
    "job_ad":               5,
    "web_mention":          6,
}


class MasterList:

    def __init__(self, path=MASTER_PATH):
        self.path    = path
        self.records = {}   # key → record dict
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = row.get("company_key", "")
                if key:
                    self.records[key] = row

    def _save(self):
        with open(self.path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=MASTER_FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(sorted(self.records.values(),
                               key=lambda r: (r["country"], r["company_name"].lower())))

    def _make_key(self, company_name, country):
        return f"{company_name.strip().lower()}|{country.strip().lower()}"

    def _is_better_evidence(self, existing_type, new_type):
        return EVIDENCE_RANK.get(new_type, 99) < EVIDENCE_RANK.get(existing_type, 99)

    def merge_run(self, targets, run_date=None):
        """
        Merge a list of target dicts from a run into the master list.
        Returns dict with counts: new, updated, unchanged.
        """
        if run_date is None:
            run_date = datetime.now().strftime("%Y-%m-%d")

        counts = {"new": 0, "updated": 0, "unchanged": 0}

        for t in targets:
            name    = t.get("target_company", "").strip()
            country = t.get("country", "").strip()
            if not name or not country:
                continue

            key = self._make_key(name, country)

            if key not in self.records:
                # Brand new company
                self.records[key] = {
                    "company_key":       key,
                    "company_name":      name,
                    "country":           country,
                    "erp_detected":      t.get("erp_detected", ""),
                    "first_seen":        run_date,
                    "last_seen":         run_date,
                    "times_seen":        "1",
                    "best_evidence_type": t.get("evidence_type", ""),
                    "best_evidence_url":  t.get("evidence_url", ""),
                    "best_evidence_title": t.get("evidence_title", ""),
                    "best_snippet":      t.get("evidence_snippet", "")[:400],
                    "source_owner":      t.get("source_owner", ""),
                    "status":            "new",
                    "status_note":       "",
                    "status_date":       "",
                }
                counts["new"] += 1

            else:
                # Seen before — update metadata
                rec = self.records[key]
                rec["last_seen"]   = run_date
                rec["times_seen"]  = str(int(rec.get("times_seen", "1")) + 1)

                # Upgrade evidence if better
                if self._is_better_evidence(
                    rec.get("best_evidence_type", ""),
                    t.get("evidence_type", ""),
                ):
                    rec["best_evidence_type"]  = t.get("evidence_type", "")
                    rec["best_evidence_url"]   = t.get("evidence_url", "")
                    rec["best_evidence_title"] = t.get("evidence_title", "")
                    rec["best_snippet"]        = t.get("evidence_snippet", "")[:400]
                    rec["source_owner"]        = t.get("source_owner", "")

                # Only mark as "seen" if not already actioned by rep
                if rec.get("status") == "new":
                    rec["status"] = "seen"

                counts["updated"] += 1

        self._save()
        return counts

    def get_new(self):
        """Return all records with status = new."""
        return [r for r in self.records.values() if r.get("status") == "new"]

    def get_all(self):
        """Return all records sorted by country then name."""
        return sorted(self.records.values(),
                      key=lambda r: (r["country"], r["company_name"].lower()))

    def get_by_status(self, status):
        return [r for r in self.records.values() if r.get("status") == status]

    def action(self, company_key, status, note=""):
        """
        Rep actions a company.
        status: actioned_crm | actioned_dismissed
        """
        if company_key in self.records:
            self.records[company_key]["status"]      = status
            self.records[company_key]["status_note"] = note
            self.records[company_key]["status_date"] = datetime.now().strftime("%Y-%m-%d")
            self._save()
            return True
        return False

    def reset_to_new(self, company_key):
        """Undo an action — put company back to new."""
        if company_key in self.records:
            self.records[company_key]["status"]      = "new"
            self.records[company_key]["status_note"] = ""
            self.records[company_key]["status_date"] = ""
            self._save()
            return True
        return False

    def stats(self):
        from collections import Counter
        statuses  = Counter(r.get("status","") for r in self.records.values())
        countries = Counter(r.get("country","") for r in self.records.values())
        return {
            "total":      len(self.records),
            "new":        statuses.get("new", 0),
            "seen":       statuses.get("seen", 0),
            "crm":        statuses.get("actioned_crm", 0),
            "dismissed":  statuses.get("actioned_dismissed", 0),
            "by_country": dict(countries),
        }

    def to_csv_bytes(self, status_filter=None):
        rows = self.get_all()
        if status_filter:
            rows = [r for r in rows if r.get("status") in status_filter]
        import io
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=MASTER_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
        return buf.getvalue().encode("utf-8")
