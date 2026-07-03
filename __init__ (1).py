"""
eCass full-fat crosswalk spider (local runner).

A Scrapy port of the scrape-ecass edge function's queue/full-fat mode — for
running from your OWN (residential) connection when the cloud edge function is
IP-blocked. Flow, all on one cookie session (Scrapy manages the cookies):

  1. GET  /Login                     → antiforgery token
  2. POST /Login/Authorise           → identity cookies
  3. GET  /ProductCheck              → a reusable token + the pharmacy accNo
  4. per search stem:
       POST /ProductCheck/AutoComplete/          → ProductLinkCode_Ids
       POST /ProductCheck                        → the per-supplier price table
       POST /ProductCheck/getNetCalculationDetails (per row, parallel via Scrapy)
     → yields one EcassOffer per (pip, supplier).

Search stems come from a file (default search_terms.txt, one per line) — export
them from your seeded queue with:
  copy (select search_term from ecass_scrape_queue order by priority)
    to stdout;

Run:
  scrapy crawl ecass -a terms_file=search_terms.txt -a max_stems=50
"""
import json
import re
import urllib.parse

import scrapy

from ecass.items import EcassOffer

BASE = "https://www.ecassweb.co.uk"

# eCass supplier registry (ids from the /PricingAlerts supplier <select>).
SUPPLIER_IDS = {
    "AAH": 1, "ACTAVIS": 2, "ALLIANCE": 3, "COLORAMA": 5, "OTC": 11,
    "PHOENIX": 12, "TRIDENT": 16, "BESTWAY": 19, "ECASSWAREHOUSE": 35,
    "MEDIHEALTH": 37,
}

TOKEN_RE = re.compile(
    r'name=["\']__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)["\']', re.I
)
ACCNO_RE = re.compile(r'\b(?:accNo|accountNumber)\s*[:=]\s*["\'](\d{3,7})["\']', re.I)
PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")


def price_to_pence(text):
    if not text:
        return None
    m = PRICE_RE.search(text.replace("£", ""))
    if not m:
        return None
    try:
        return round(float(m.group(0).replace(",", "")) * 100)
    except ValueError:
        return None


def nz(v):
    """eCass nulls are '' and 'Unknown'."""
    if v is None:
        return None
    s = str(v).strip()
    return None if (not s or s.lower() == "unknown") else s


class EcassSpider(scrapy.Spider):
    name = "ecass"

    def __init__(self, terms_file="search_terms.txt", max_stems=None,
                 max_linkcodes=40, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.terms_file = terms_file
        self.max_stems = int(max_stems) if max_stems else None
        self.max_linkcodes = int(max_linkcodes)
        self.token = None
        self.acc_no = None
        self.seen_link = set()

    # ── credentials ────────────────────────────────────────────────────────
    # Prefer Scrapy settings (works on Zyte, where you set them in the project's
    # Raw settings) and fall back to the local .env / environment.
    @property
    def _user(self):
        import os
        return self.settings.get("ECASS_USER") or os.getenv("ECASS_USER", "")

    @property
    def _pass(self):
        import os
        return self.settings.get("ECASS_PASS") or os.getenv("ECASS_PASS", "")

    def _load_terms(self):
        terms = []
        try:
            with open(self.terms_file, encoding="utf-8") as fh:
                terms = [ln.strip() for ln in fh if ln.strip() and len(ln.strip()) >= 3]
        except FileNotFoundError:
            terms = []
        # No local file (e.g. on Zyte) → pull due stems from the Supabase queue.
        if not terms:
            terms = self._terms_from_supabase()
        if not terms:
            self.logger.error(
                "No search terms — add %s or set SUPABASE_URL/SUPABASE_SERVICE_KEY "
                "so stems can be read from ecass_scrape_queue.", self.terms_file,
            )
            return []
        # de-dup, keep order
        seen, out = set(), []
        for t in terms:
            k = t.lower()
            if k not in seen:
                seen.add(k)
                out.append(t)
        return out[: self.max_stems] if self.max_stems else out

    def _terms_from_supabase(self):
        import os
        import requests
        url = (self.settings.get("SUPABASE_URL") or os.getenv("SUPABASE_URL", "")).rstrip("/")
        key = self.settings.get("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_KEY", "")
        if not url or not key:
            return []
        limit = self.max_stems or 5000
        endpoint = (
            f"{url}/rest/v1/ecass_scrape_queue"
            f"?select=search_term&status=in.(pending,error)"
            f"&order=priority.asc,next_due_at.asc&limit={limit}"
        )
        try:
            resp = requests.get(
                endpoint, timeout=30,
                headers={"apikey": key, "Authorization": f"Bearer {key}"},
            )
            if resp.status_code >= 300:
                self.logger.error("queue fetch failed (%s): %s", resp.status_code, resp.text[:200])
                return []
            rows = resp.json()
            self.logger.info("Pulled %d stems from ecass_scrape_queue.", len(rows))
            return [r["search_term"] for r in rows if r.get("search_term")]
        except Exception as exc:
            self.logger.error("queue fetch error: %s", exc)
            return []

    # ── 1. login page ──────────────────────────────────────────────────────
    def start_requests(self):
        if not self._user or not self._pass:
            self.logger.error("Set ECASS_USER and ECASS_PASS (in .env).")
            return
        yield scrapy.Request(f"{BASE}/Login", callback=self.parse_login, dont_filter=True)

    def parse_login(self, response):
        m = TOKEN_RE.search(response.text)
        if not m:
            self.logger.error("No antiforgery token on /Login")
            return
        yield scrapy.FormRequest(
            f"{BASE}/Login/Authorise",
            formdata={
                "Username": self._user,
                "Password": self._pass,
                "__RequestVerificationToken": m.group(1),
            },
            headers={"Referer": f"{BASE}/Login", "Origin": BASE},
            callback=self.after_login,
            dont_filter=True,
        )

    # ── 2. verify login, prime token + accNo ───────────────────────────────
    def after_login(self, response):
        # A failed login re-renders /Login; success lands on / then we fetch the
        # ProductCheck page (which redirects to /Login if we're NOT authed).
        yield scrapy.Request(
            f"{BASE}/ProductCheck", callback=self.primed, dont_filter=True,
            headers={"Referer": f"{BASE}/"},
        )

    def primed(self, response):
        if "/Login" in response.url:
            self.logger.error("Login failed — check ECASS_USER / ECASS_PASS.")
            return
        tok = TOKEN_RE.search(response.text)
        self.token = tok.group(1) if tok else None
        acc = ACCNO_RE.search(response.text)
        self.acc_no = acc.group(1) if acc else None
        if not self.token:
            self.logger.error("No ProductCheck token after login.")
            return
        self.logger.info("Logged in. accNo=%s. Harvesting…", self.acc_no)

        for stem in self._load_terms():
            yield scrapy.Request(
                f"{BASE}/ProductCheck/AutoComplete/",
                method="POST",
                body=json.dumps({"prefix": stem}),
                headers={
                    "Content-Type": "application/json; charset=UTF-8",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"{BASE}/ProductCheck",
                    "Origin": BASE,
                },
                callback=self.parse_autocomplete,
                cb_kwargs={"stem": stem},
                dont_filter=True,
            )

    # ── 3. autocomplete → per link code price POST ─────────────────────────
    def parse_autocomplete(self, response, stem):
        try:
            items = json.loads(response.text)
        except json.JSONDecodeError:
            return
        if not isinstance(items, list):
            return
        n = 0
        for it in items:
            val = it.get("val") if isinstance(it, dict) else None
            if val is None or str(val) in self.seen_link:
                continue
            self.seen_link.add(str(val))
            n += 1
            if n > self.max_linkcodes:
                break
            name = (it.get("label") or stem).strip()
            yield scrapy.FormRequest(
                f"{BASE}/ProductCheck",
                formdata={
                    "ProductLinkCode_Id": str(val),
                    "hfProductLinkCode_IdRemove": "",
                    "hfProductName": name,
                    "ProductName": name,
                    "CustomProductId": "",
                    "removeMarketResults": "false",
                    "txtProductSearch": stem,
                    "__RequestVerificationToken": self.token,
                },
                headers={"Referer": f"{BASE}/ProductCheck", "Origin": BASE},
                callback=self.parse_product,
                cb_kwargs={"stem": stem, "link_code_id": str(val)},
                dont_filter=True,
            )

    # ── 4. parse the per-supplier price table ──────────────────────────────
    def parse_product(self, response, stem, link_code_id):
        if self.acc_no is None:
            acc = ACCNO_RE.search(response.text)
            if acc:
                self.acc_no = acc.group(1)

        tariff_pence = after_claw = None
        tm = re.search(r"Trade\s*or\s*Tariff\s*Price:\s*&#x[aA]3;?\s*([\d.,]+)", response.text)
        if not tm:
            tm = re.search(r"Trade\s*or\s*Tariff\s*Price:\s*£?\s*([\d.,]+)", response.text)
        if tm:
            tariff_pence = price_to_pence(tm.group(1))
        cm = re.search(r"After\s*Clawback:\s*(?:&#x[aA]3;)?£?\s*([\d.,]+)", response.text)
        if cm:
            after_claw = price_to_pence(cm.group(1))

        rows = response.css("tbody#productInformationData tr")
        for row in rows:
            tds = row.css("td")
            if len(tds) < 6:
                continue
            prodsup = row.css("td.prodSup")
            supplier = (prodsup.css("span:not(.prodCode)::text").get() or "").strip()
            pip = (prodsup.css("span.prodCode::text").get() or "").strip()
            if not supplier or not pip:
                continue

            # price cell = the one carrying the net-calc info button (else col 2)
            price_td = row.css("td:has(button.netcalcinfo)") or (tds[2:3])
            price_text = " ".join(price_td.css("::text").getall()).strip() if price_td else ""

            stock_td = row.css("td.stockLevel")
            stock_title = (stock_td.css("i::attr(title)").get() or "").strip()
            stock_text = " ".join(stock_td.css("::text").getall()).strip()
            last_order_response = stock_title or stock_text or None
            stock_html = stock_td.get() or ""
            positive = None
            if "color-green" in stock_html or "positive" in (stock_title or "").lower():
                positive = True
            elif "color-red" in stock_html or "color-error" in stock_html or "negative" in (stock_title or "").lower():
                positive = False

            below_trade = self._dot(tds[4]) if len(tds) > 4 else None
            below_claw = self._dot(tds[5]) if len(tds) > 5 else None
            desc = " ".join(tds[0].css("::text").getall()).strip()

            offer = {
                "pip_code": pip,
                "supplier_code": re.sub(r"\s+", "", supplier).upper(),
                "supplier_name": supplier,
                "supplier_id": SUPPLIER_IDS.get(re.sub(r"\s+", "", supplier).upper()),
                "product_link_code_id": link_code_id,
                "ecass_prod_code": pip,
                "product_description": desc or None,
                "net_price_pence": price_to_pence(price_text),
                "in_stock": positive,
                "last_order_response": last_order_response,
                "last_order_positive": positive,
                "below_trade_tariff": below_trade,
                "below_clawback": below_claw,
                "trade_tariff_pence": tariff_pence,
                "after_clawback_pence": after_claw,
                "last_seen_date": None,  # set by feed/DB default (now)
            }

            if not self.acc_no:
                yield self._finish(offer)  # no accNo → can't net-calc; ship table fields
                continue

            is_brand_deal = "brand deal product" in (last_order_response or "").lower()
            q = urllib.parse.urlencode({
                "accNo": self.acc_no, "prodCode": pip, "supplier": supplier,
                "isBrandDeal": "true" if is_brand_deal else "false",
            })
            yield scrapy.Request(
                f"{BASE}/ProductCheck/getNetCalculationDetails?{q}",
                method="POST",
                body="",
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"{BASE}/ProductCheck",
                    "Origin": BASE,
                },
                callback=self.parse_netcalc,
                cb_kwargs={"offer": offer},
                dont_filter=True,
            )

    @staticmethod
    def _dot(td):
        html = td.get() or ""
        if "color-green" in html or "text-success" in html:
            return "Yes"
        if "color-red" in html or "color-error" in html or "text-danger" in html:
            return "No"
        txt = " ".join(td.css("::text").getall()).lower()
        if "yes" in txt:
            return "Yes"
        if "no" in txt:
            return "No"
        return None

    # ── 5. fold in net-calc detail, emit ───────────────────────────────────
    def parse_netcalc(self, response, offer):
        try:
            d = json.loads(response.text)
        except json.JSONDecodeError:
            d = {}
        spd = nz(d.get("spM_SupplierProductDesc")) or nz(d.get("fullDesc"))
        offer["supplier_product_description"] = spd
        offer["supplier_internal_code"] = nz(d.get("spM_SupplierInternalProductCode"))
        offer["price_category"] = nz(d.get("supplierProductCategory_Desc")) or nz(d.get("supplierPriceTierProductCategory_Desc"))
        offer["price_tier"] = nz(d.get("supplierPriceTierLevel_Desc"))
        offer["price_tier_id"] = nz(d.get("supplierPriceTierLevel_Id"))
        offer["wda_discount"] = nz(d.get("spM_WDADiscount"))
        offer["mds_discount"] = nz(d.get("spM_MDSDiscount"))
        offer["rebate"] = nz(d.get("rebate"))
        offer["rebate_type"] = nz(d.get("rebateType"))
        np = price_to_pence(nz(d.get("price")) or "")
        if np is not None:
            offer["net_price_pence"] = np
        offer["list_price_pence"] = price_to_pence(nz(d.get("spM_ListPrice")) or "")
        # manufacturer marker embedded in the desc, e.g. "… [MORN] 28" → Morningside
        if spd:
            marks = re.findall(r"\[([^\]]+)\]", spd)
            if marks:
                offer["manufacturer"] = marks[-1].strip()
        yield self._finish(offer)

    @staticmethod
    def _finish(offer):
        item = EcassOffer()
        for k, v in offer.items():
            item[k] = v
        # drop keys we set to None that map to DB-default columns
        if not item.get("last_seen_date"):
            item.pop("last_seen_date", None)
        return item
