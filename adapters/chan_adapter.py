"""
4chan adapter for OSINT collection.

Uses 4chan's public JSON API to fetch posts from boards.
The API is free, requires no authentication, and is well-documented.

API Docs: https://github.com/4chan/4chan-API

IMPORTANT: This adapter is configured to ONLY access SFW (blue) boards.
NSFW boards are explicitly blocked to keep the project demo-safe.

Rate Limits:
    - 1 request per second for the same endpoint
    - No authentication required
    - Threads are ephemeral - they disappear when pruned
"""

import logging
import time
from datetime import datetime
from typing import List, Dict, Set, Optional
from html import unescape
import re

import requests

# TODO: pip install requests
# import requests

from adapters.base import SourceAdapter
from models.post import OSINTPost
from config import CHAN_SFW_BOARDS

logger = logging.getLogger(__name__)


class ChanAdapter(SourceAdapter):
    """
    Fetches posts from 4chan using the public JSON API.

    This adapter ONLY accesses SFW boards for safety.

    Attributes:
        source_name: Identifier for this source ("4chan")
        boards: List of board names to monitor (SFW only)
        base_url: Base URL for 4chan API
    """

    source_name = "4chan"

    # Base URLs for 4chan API
    API_BASE = "https://a]a]a]a.4cdn.org"
    THREAD_URL_TEMPLATE = "https://boards.4chan.org/{board}/thread/{thread_id}"

    # NSFW boards - these are BLOCKED regardless of user configuration
    # This is a safety measure to prevent accidental access
    NSFW_BOARDS = {
        "b", "r9k", "pol", "bant", "soc", "s4s",  # Random/social
        "s", "hc", "hm", "h", "e", "u", "d", "y", "t", "hr", "gif",  # Adult
        "aco", "r",  # Adult content
    }

    def __init__(self, boards: Optional[List[str]] = None):
        """
        Initialize the 4chan adapter.

        Args:
            boards: List of boards to monitor. Must be SFW boards.
                   Defaults to config.CHAN_SFW_BOARDS.

        Raises:
            ValueError: If any NSFW boards are specified
        """
        requested_boards = set(boards or CHAN_SFW_BOARDS)

        # Safety check: Block NSFW boards
        nsfw_requested = requested_boards & self.NSFW_BOARDS
        if nsfw_requested:
            raise ValueError(
                f"NSFW boards are blocked for safety: {nsfw_requested}. "
                "Remove these from your configuration."
            )

        self.boards = list(requested_boards)
        self._last_request_time = 0

    def _rate_limit(self):
        """
        Enforce rate limiting (1 request per second).

        Call this before each API request.
        """
        elapsed = time.time() - self._last_request_time
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        self._last_request_time = time.time()

    def fetch(self, limit_per_board: int = 50) -> List[OSINTPost]:
        """
        Fetch recent posts from all configured boards.

        Args:
            limit_per_board: Maximum posts to fetch per board

        Returns:
            List of OSINTPost objects (keywords not yet matched)
        """
        posts = []

        for board in self.boards:
            try:
                board_posts = self._fetch_board(board, limit_per_board)
                posts.extend(board_posts)
                logger.info(f"Fetched {len(board_posts)} posts from /{board}/")
            except Exception as e:
                logger.error(f"Error fetching /{board}/: {e}")
                continue

        return posts

    def _fetch_board(self, board: str, limit: int) -> List[OSINTPost]:
        """
        Fetch posts from a single board.

        Strategy:
            1. Fetch the catalog (list of all threads)
            2. Sort threads by reply count or bump time
            3. Fetch the most active threads
            4. Extract OP and recent replies

        Args:
            board: Board name (without slashes)
            limit: Maximum posts to return

        Returns:
            List of OSINTPost objects
        """
        posts = []

        self._rate_limit()
        catalog_url = f"{self.API_BASE}/{board}/catalog.json"
        response = requests.get(catalog_url)
        response.raise_for_status()
        catalog = response.json()

        # Catalog is a list of pages, each page has a list of threads
        threads = []
        for page in catalog:
            threads.extend(page.get("threads", []))

        # Sort by reply count (most active first)
        threads.sort(key=lambda t: t.get("replies", 0), reverse=True)

        # Fetch top threads
        for thread in threads[:10]:  # Limit thread fetches
            thread_posts = self._fetch_thread(board, thread["no"])
            posts.extend(thread_posts)
            if len(posts) >= limit:
                break

        return posts[:limit]

    def _fetch_thread(self, board: str, thread_id: int) -> List[OSINTPost]:
        """
        Fetch all posts from a specific thread.

        Args:
            board: Board name
            thread_id: Thread number (OP post number)

        Returns:
            List of OSINTPost objects
        """
        posts = []

        self._rate_limit()
        thread_url = f"{self.API_BASE}/{board}/thread/{thread_id}.json"
        response = requests.get(thread_url)
        response.raise_for_status()
        thread_data = response.json()

        for post_data in thread_data.get("posts", []):
            post = self._convert_post(post_data, board, thread_id)
            if post:  # May return None if filtered
                posts.append(post)

        return posts

    def _convert_post(self, post_data: Dict, board: str, thread_id: int) -> Optional[OSINTPost]:
        """
        Convert a 4chan API post object to an OSINTPost.

        Args:
            post_data: Raw post data from API
            board: Board name
            thread_id: Parent thread ID

        Returns:
            OSINTPost object, or None if post should be skipped
        """

        # Extract and clean the comment text
        raw_comment = post_data.get("com", "")
        clean_text = self._clean_html(raw_comment)

        # Also include the subject line if present (often contains key info)
        subject = post_data.get("sub", "")
        if subject:
            clean_text = f"{subject}\n\n{clean_text}"

        # Skip empty posts (image-only posts have no text)
        if not clean_text.strip():
            return None

        # Convert Unix timestamp to datetime
        timestamp = datetime.utcfromtimestamp(post_data.get("time", 0))

        return OSINTPost(
            post_id=f"4chan_{board}_{post_data['no']}",
            date=timestamp,
            username="Anonymous",  # 4chan is anonymous
            text=clean_text,
            source=f"4chan/{board}",
            url=self.THREAD_URL_TEMPLATE.format(board=board, thread_id=thread_id),
            matched_keywords=[],
            categories=[]
        )

        pass

    def _clean_html(self, html_text: str) -> str:
        """
        Clean HTML from 4chan post text.

        4chan comments contain HTML entities and tags that need to be
        converted to plain text for analysis.

        Args:
            html_text: Raw HTML text from API

        Returns:
            Clean plain text
        """
        if not html_text:
            return ""

        # TODO: Implement HTML cleaning
        #
        # Decode HTML entities (&gt; -> >, etc.)
        text = unescape(html_text)

        # Convert <br> to newlines
        text = re.sub(r'<br\s*/?>', '\n', text)

        # Remove greentext quotes formatting but keep the text
        text = re.sub(r'<span class="quote">(.*?)</span>', r'\1', text)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Normalize whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        return text

# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Example usage
    try:
        adapter = ChanAdapter(boards=["news", "n"])
        posts = adapter.fetch(limit_per_board=20)
        print(f"Fetched {len(posts)} total posts")

        for post in posts[:5]:
            print(f"- [{post.source}] {post.text[:100]}...")

    except ValueError as e:
        print(f"Configuration error: {e}")
