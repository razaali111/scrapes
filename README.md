# eCass full-fat crosswalk scraper (Scrapy, local runner)

A standalone [Scrapy](https://scrapy.org) port of the `scrape-ecass` edge
function's full-fat harvest — for running from **your own connection** when the
cloud edge function gets IP-blocked (eCass sees the datacenter IP; a residential
IP looks like a normal pharmacist).

It logs into `ecassweb.co.uk`, and for each search stem captures the whole
per-`(pip, supplier)` crosswalk — wholesaler internal code, price tier/rule,
category, net/list price, in-stock, below trade/clawback — into
`public.ecass_offers` (and/or a local JSONL file). Same table the app reads.

## Setup

```bash
cd tools/ecass-scrapy
python3 -m venv .venv && source .venv/bin/activate    # optional but recommended
pip install -r requirements.txt
cp .env.example .env          # then edit .env
```

Fill in `.env`:
- `ECASS_USER` / `ECASS_PASS` — a branch eCass login (Boulevard/Ovenden).
- `SUPABASE_URL` + `SUPABASE_SERVICE_KEY` — **optional**. Set both to upsert
  straight into `ecass_offers`. Use the **service-role** key (Dashboard →
  Settings → API). Leave blank to write only `output/ecass_offers_*.jsonl`.

## Search stems

The spider reads stems from a file (default `search_terms.txt`, one per line).
Export the ones already seeded in your queue from the Supabase SQL editor:

```sql
copy (select search_term from ecass_scrape_queue order by priority)
  to stdout;
```

Paste the output into `search_terms.txt`. (Or hand-write a few molecule names to
test: `metformin 500mg`, `lansoprazole 30mg`, …)

## Run

```bash
# small test first — 20 stems, low concurrency
scrapy crawl ecass -a terms_file=search_terms.txt -a max_stems=20

# full run
scrapy crawl ecass -a terms_file=search_terms.txt
```

Tuning (also settable in `.env`):
- `ECASS_CONCURRENCY` (default 6) — Scrapy parallelises the per-row net-calc
  lookups; raise from a residential IP, lower if eCass 429/500s.
- `ECASS_DELAY` (default 0.15s) — polite gap; AutoThrottle adapts around it.
- `-a max_stems=N` — cap stems this run. `-a max_linkcodes=N` — cap products per
  stem (default 40).

Resume/idempotent: upserts are on `(pip_code, supplier_code)`, so re-running
refreshes rather than duplicates. Scrapy's HTTP cache is off by default.

## dm+d / barcode enrichment (after the run)

The spider captures eCass-native fields; fill the dm+d columns (barcode, SNOMED,
VMP/VMPP, classification) from your own data with one SQL pass:

```sql
update public.ecass_offers o
   set snomed_code = m.snomed_code,
       ampp_code   = m.snomed_code,
       vmp_code    = m.vmp_code,
       vmpp_code   = m.vmpp_code,
       gtin        = m.gtin,
       manufacturer = coalesce(o.manufacturer, m.manufacturer, m.brand_name)
  from public.master_product_lookup m
 where m.pip_code = o.pip_code
   and o.snomed_code is null;
```

## Running on Zyte Scrapy Cloud (instead of locally)

⚠️ **Zyte Cloud uses datacenter IPs — the same kind eCass blocks.** Deploying
as-is will get blocked just like the edge function. To beat it you need Zyte's
**Smart Proxy** (paid add-on) for residential IPs, and because eCass needs a
stable login session you must run it as a **single sticky session** (one IP for
the whole crawl), not per-request rotation. A local run on your own connection
avoids all of this.

If you still want Zyte:

1. Deploy from this folder (needs Python + `pip install shub` on any machine —
   this only uploads the code; the crawl runs on Zyte):
   ```bash
   cd tools/ecass-scrapy
   pip install shub
   shub login            # paste your Zyte API key
   shub deploy 869022    # project id from the Zyte URL
   ```
   (GitHub deploy won't work — the project lives in a subfolder, not the repo
   root; use `shub`.)
2. In Zyte → your project → **Settings → Raw settings**, add:
   `ECASS_USER`, `ECASS_PASS`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, and to
   enable residential IPs `ZYTE_SMARTPROXY_ENABLED=True` + `ZYTE_SMARTPROXY_APIKEY`.
   Tune with `CONCURRENT_REQUESTS`.
3. Upload your stems: put `search_terms.txt` in the project, or pass a few via
   `-a max_stems=...` for a test job.
4. **Dashboard → Run** the `ecass` spider. Items land in Zyte's item store
   (downloadable) and, if the Supabase settings are set, upsert into `ecass_offers`.

## Notes / safety

- **Credentials:** `.env` is gitignored. Never commit real logins or the service
  key. Rotate anything that may have been exposed.
- **One login session** — Scrapy runs it all on one cookie jar; don't launch two
  crawls on the same eCass account at once.
- **Load / blocking:** this is a real, authenticated harvest against eCass under
  a named pharmacy account — same account-flagging caveats as the edge function.
  Keep concurrency sane; stop if you see sustained 429s.
- The spider mirrors the reverse-engineered flow documented in
  `docs/ecass-scraper-review.md`.
