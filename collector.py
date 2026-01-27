"""
Main OSINT collector orchestrator.

This module ties together all components:
    - Source adapters (Reddit, 4chan)
    - Content filtering
    - Keyword matching
    - Storage

It provides both single-run and continuous collection modes.
"""

import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

from models.post import OSINTPost
from adapters.reddit_adapter import RedditAdapter
from adapters.chan_adapter import ChanAdapter
from processing.content_filter import ContentFilter
from processing.keyword_matcher import KeywordMatcher
from storage.database import Storage
from config import collection_config, REDDIT_SUBREDDITS, CHAN_SFW_BOARDS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class OSINTCollector:
    """
    Main orchestrator for OSINT collection.

    Coordinates fetching from multiple sources, filtering,
    keyword matching, and storage.

    Attributes:
        adapters: List of source adapters
        filter: Content filter instance
        matcher: Keyword matcher instance
        storage: Storage instance
    """

    def __init__(
        self,
        reddit_credentials: Optional[Dict[str, str]] = None,
        enable_reddit: bool = True,
        enable_4chan: bool = True,
        keywords_path: str = "Keywords.csv",
        db_path: str = "osint_data.db"
    ):
        """
        Initialize the collector.

        Args:
            reddit_credentials: Dict with "client_id" and "client_secret"
            enable_reddit: Whether to enable Reddit collection
            enable_4chan: Whether to enable 4chan collection
            keywords_path: Path to keywords CSV
            db_path: Path to SQLite database
        """
        self.adapters = []

        # Initialize Reddit adapter
        if enable_reddit and reddit_credentials:
            try:
                reddit_adapter = RedditAdapter(
                    client_id=reddit_credentials["client_id"],
                    client_secret=reddit_credentials["client_secret"]
                )
                self.adapters.append(reddit_adapter)
                logger.info("Reddit adapter initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Reddit adapter: {e}")

        # Initialize 4chan adapter
        if enable_4chan:
            try:
                chan_adapter = ChanAdapter(boards=CHAN_SFW_BOARDS)
                self.adapters.append(chan_adapter)
                logger.info("4chan adapter initialized")
            except Exception as e:
                logger.error(f"Failed to initialize 4chan adapter: {e}")

        # Initialize filter, matcher, and storage
        self.filter = ContentFilter()
        self.matcher = KeywordMatcher(keywords_path)
        self.storage = Storage(db_path)

        logger.info(f"Collector initialized with {len(self.adapters)} adapters")

    def collect_once(self) -> Dict[str, int]:
        """
        Run a single collection cycle.

        Fetches from all sources, filters, matches keywords, and stores.

        Returns:
            Statistics dict with counts
        """
        stats = {
            "fetched": 0,
            "filtered_out": 0,
            "matched": 0,
            "stored": 0,
            "duplicates": 0,
            "errors": 0,
            "by_source": {},
            "by_domain": {}
        }

        all_posts = []

        # Fetch from all adapters
        for adapter in self.adapters:
            try:
                logger.info(f"Fetching from {adapter.source_name}...")
                posts = adapter.fetch()
                all_posts.extend(posts)
                stats["by_source"][adapter.source_name] = len(posts)
                logger.info(f"Fetched {len(posts)} posts from {adapter.source_name}")
            except Exception as e:
                logger.error(f"Error fetching from {adapter.source_name}: {e}")
                stats["errors"] += 1

        stats["fetched"] = len(all_posts)

        if not all_posts:
            logger.warning("No posts fetched from any source")
            return stats

        # Filter posts
        logger.info("Filtering posts...")
        filtered_posts, filtered_count = self.filter.filter_batch(all_posts)
        stats["filtered_out"] = filtered_count

        # Match keywords
        logger.info("Matching keywords...")
        matched_posts, match_stats = self.matcher.match_batch(filtered_posts)
        stats["matched"] = match_stats["posts_with_matches"]
        stats["by_domain"] = match_stats["domain_counts"]

        # Only store posts that matched at least one keyword
        posts_to_store = [p for p in matched_posts if p.matched_keywords]

        # Store posts
        logger.info(f"Storing {len(posts_to_store)} posts...")
        store_results = self.storage.store_batch(posts_to_store)
        stats["stored"] = store_results["stored"]
        stats["duplicates"] = store_results["duplicates"]

        # Log summary
        logger.info(
            f"Collection complete: {stats['fetched']} fetched, "
            f"{stats['filtered_out']} filtered, "
            f"{stats['matched']} matched, "
            f"{stats['stored']} stored, "
            f"{stats['duplicates']} duplicates"
        )

        return stats

    def collect_continuous(self, interval_seconds: int = 300):
        """
        Run continuous collection.

        Collects at regular intervals until interrupted.

        Args:
            interval_seconds: Seconds between collection cycles
        """
        logger.info(f"Starting continuous collection (interval: {interval_seconds}s)")
        logger.info("Press Ctrl+C to stop")

        cycle = 0
        try:
            while True:
                cycle += 1
                logger.info(f"=== Collection cycle {cycle} ===")

                try:
                    stats = self.collect_once()

                    # Periodic cleanup
                    if cycle % 10 == 0:  # Every 10 cycles
                        cleaned = self.storage.cleanup_old_posts()
                        if cleaned > 0:
                            logger.info(f"Cleaned up {cleaned} old posts")

                except Exception as e:
                    logger.error(f"Error in collection cycle: {e}")

                logger.info(f"Sleeping for {interval_seconds} seconds...")
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("Collection stopped by user")

    def get_stats(self) -> Dict:
        """
        Get current collection statistics.

        Returns:
            Dict with database stats and adapter info
        """
        db_stats = self.storage.get_stats()
        return {
            "database": db_stats,
            "adapters": [a.source_name for a in self.adapters],
            "adapter_count": len(self.adapters)
        }


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """
    Command-line interface for the collector.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="OSINT Collector - Gather and analyze public data"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single collection cycle and exit"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Collection interval in seconds (default: 300)"
    )
    parser.add_argument(
        "--no-reddit",
        action="store_true",
        help="Disable Reddit collection"
    )
    parser.add_argument(
        "--no-4chan",
        action="store_true",
        help="Disable 4chan collection"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics and exit"
    )

    args = parser.parse_args()

    # TODO: Load Reddit credentials from environment or config file
    # reddit_credentials = {
    #     "client_id": os.environ.get("REDDIT_CLIENT_ID"),
    #     "client_secret": os.environ.get("REDDIT_CLIENT_SECRET")
    # }

    reddit_credentials = None  # Disabled until credentials are configured

    collector = OSINTCollector(
        reddit_credentials=reddit_credentials,
        enable_reddit=not args.no_reddit and reddit_credentials is not None,
        enable_4chan=not args.no_4chan
    )

    if args.stats:
        stats = collector.get_stats()
        print("\n=== OSINT Collector Statistics ===")
        print(f"Active adapters: {', '.join(stats['adapters'])}")
        print(f"Total posts: {stats['database']['total_posts']}")
        print(f"Unique sources: {stats['database']['unique_sources']}")
        print(f"Keywords matched: {stats['database']['unique_keywords_matched']}")
        if stats['database']['date_range']['min']:
            print(f"Date range: {stats['database']['date_range']['min']} to {stats['database']['date_range']['max']}")
        return

    if args.once:
        stats = collector.collect_once()
        print(f"\nCollection complete. Stored {stats['stored']} new posts.")
    else:
        collector.collect_continuous(interval_seconds=args.interval)


if __name__ == "__main__":
    main()
