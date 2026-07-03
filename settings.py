import os
import logging

import requests

logger = logging.getLogger(__name__)


class SupabaseUpsertPipeline:
    """Upsert offers into public.ecass_offers via PostgREST, batched.

    No-ops (leaving just the JSONL feed) when SUPABASE_URL / SUPABASE_SERVICE_KEY
    aren't set, so you can dry-run to a file first.
    """

    BATCH = 200

    def open_spider(self, spider):
        # Prefer Scrapy settings (Zyte project settings) then the local env.
        s = spider.settings
        self.url = (s.get("SUPABASE_URL") or os.getenv("SUPABASE_URL", "")).rstrip("/")
        self.key = s.get("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_KEY", "")
        self.enabled = bool(self.url and self.key)
        self.buffer = []
        self.written = 0
        self.errors = 0
        if not self.enabled:
            spider.logger.warning(
                "SupabaseUpsertPipeline disabled (SUPABASE_URL / SUPABASE_SERVICE_KEY "
                "not set) — writing JSONL only."
            )

    def process_item(self, item, spider):
        if self.enabled:
            self.buffer.append(dict(item))
            if len(self.buffer) >= self.BATCH:
                self._flush(spider)
        return item

    def close_spider(self, spider):
        if self.enabled:
            self._flush(spider)
            spider.logger.info(
                "Supabase upsert done: %d rows written, %d batch errors.",
                self.written, self.errors,
            )

    def _flush(self, spider):
        if not self.buffer:
            return
        endpoint = f"{self.url}/rest/v1/ecass_offers?on_conflict=pip_code,supplier_code"
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        try:
            resp = requests.post(endpoint, headers=headers, json=self.buffer, timeout=60)
            if resp.status_code >= 300:
                self.errors += 1
                spider.logger.error(
                    "ecass_offers upsert failed (%s): %s",
                    resp.status_code, resp.text[:400],
                )
            else:
                self.written += len(self.buffer)
        except Exception as exc:  # network hiccup — keep scraping, JSONL is the backstop
            self.errors += 1
            spider.logger.error("ecass_offers upsert error: %s", exc)
        finally:
            self.buffer = []
