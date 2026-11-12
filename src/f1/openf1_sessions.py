"""
This is our reusable API client. It is responsible for making requests, streaming responses to disk and handling retries
"""

import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests


class OpenF1Client:
    base_url = "https://api.openf1.org/v1"

    def __init__(self, min_interval=2, max_attempts=5):
        self.min_interval = min_interval
        self.max_attempts = max_attempts
        self.http = requests.Session()
        self.last_request_time = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.http.close()

    def _rate_limit(self):
        """space out API requests"""
        if self.last_request_time is not None:
            elapsed = time.monotonic() - self.last_request_time

            wait = self.min_interval - elapsed

            if wait > 0:
                time.sleep(wait)

        self.last_request_time = time.monotonic()

    def _retry_delay(self, attempt, status_code=None, retry_after=None):
        if retry_after:
            try:
                seconds = float(retry_after)
                if seconds > 0:
                    return seconds
            except ValueError:
                try:
                    delay = parsedate_to_datetime(retry_after)

                    seconds = (delay.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds()

                    if seconds > 0:
                        return seconds

                except (TypeError, ValueError):
                    pass

        backoff = min(120, 2**attempt)

        if status_code == 429:
            backoff = min(60, backoff)

        jitter = random.uniform(0, 1)

        return backoff + jitter

    def download_json(self, endpoint, params, localfile):
        url = f"{self.base_url}/{endpoint}"
        localfile = Path(localfile)

        localfile.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, self.max_attempts + 1):
            self._rate_limit()

            try:
                with self.http.get(url=url, params=params, stream=True, timeout=(10, 120)) as response:
                    status_code = response.status_code

                    if status_code == 429 or 500 <= status_code < 600:
                        if attempt == self.max_attempts:
                            response.raise_for_status()

                        delay = self._retry_delay(
                            attempt=attempt, status_code=status_code, retry_after=response.headers.get("Retry-After")
                        )

                        print(f"HTTP {status_code}. Attempt {attempt}/{self.max_attempts}. Retrying in {delay:.1f}s")

                    else:
                        response.raise_for_status()

                        with localfile.open("wb") as file:
                            for chunk in response.iter_content(chunk_size=2048 * 1024):
                                if chunk:
                                    file.write(chunk)

                        return localfile

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as error:
                if attempt == self.max_attempts:
                    raise

                delay = self._retry_delay(attempt)

                print(f"Network error: {error}. Retrying in {delay:.1f}s")

            time.sleep(delay)

        raise RuntimeError(f"Failed to download {endpoint}")
