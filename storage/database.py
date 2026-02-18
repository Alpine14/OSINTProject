"""
Storage module for OSINT collection.

Handles persistence of collected posts using SQLite.
SQLite is chosen for simplicity - no external database server required,
and the entire database is a single file that's easy to back up.

For larger deployments, this could be swapped for PostgreSQL or similar.
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import contextmanager

from models.post import OSINTPost
from config import storage_config

logger = logging.getLogger(__name__)


class Storage:
    """
    SQLite-based storage for OSINT posts.

    Provides methods for storing, retrieving, and querying posts.
    Handles schema creation and data retention policies.

    Attributes:
        db_path: Path to the SQLite database file
        retention_days: Days to retain posts (0 = indefinite)
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        retention_days: Optional[int] = None
    ):
        """
        Initialize storage.

        Args:
            db_path: Path to database file (defaults to config)
            retention_days: Retention period (defaults to config)
        """
        self.db_path = db_path or storage_config.database_path
        self.retention_days = retention_days if retention_days is not None else storage_config.retention_days

        self._init_database()

    @contextmanager
    def _get_connection(self):
        """
        Context manager for database connections.

        Ensures connections are properly closed after use.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_database(self):
        """
        Initialize database schema.

        Creates tables if they don't exist.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Main posts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id TEXT UNIQUE NOT NULL,
                    date TIMESTAMP NOT NULL,
                    username TEXT,
                    text TEXT NOT NULL,
                    source TEXT NOT NULL,
                    url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Keywords matched for each post (many-to-many)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS post_keywords (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    FOREIGN KEY (post_id) REFERENCES posts(post_id)
                )
            """)

            # Categories for each post
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS post_categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id TEXT NOT NULL,
                    category TEXT NOT NULL,
                    FOREIGN KEY (post_id) REFERENCES posts(post_id)
                )
            """)

            # Indexes for common queries
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_date ON posts(date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_source ON posts(source)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_keywords_keyword ON post_keywords(keyword)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_categories_category ON post_categories(category)")

            # View for easy browsing with all data in one place
            cursor.execute("DROP VIEW IF EXISTS posts_full")
            cursor.execute("""
                CREATE VIEW posts_full AS
                SELECT
                    p.id,
                    p.post_id,
                    p.date,
                    p.username,
                    p.text,
                    p.source,
                    p.url,
                    GROUP_CONCAT(DISTINCT pk.keyword) AS matched_keywords,
                    GROUP_CONCAT(DISTINCT pc.category) AS categories,
                    p.created_at
                FROM posts p
                LEFT JOIN post_keywords pk ON p.post_id = pk.post_id
                LEFT JOIN post_categories pc ON p.post_id = pc.post_id
                GROUP BY p.id
            """)

            logger.info(f"Database initialized: {self.db_path}")

    def store(self, post: OSINTPost) -> bool:
        """
        Store a single post.

        Args:
            post: The post to store

        Returns:
            True if stored successfully, False if duplicate
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # Insert main post
                cursor.execute("""
                    INSERT OR IGNORE INTO posts (post_id, date, username, text, source, url)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    post.post_id,
                    post.date.isoformat(),
                    post.username,
                    post.text,
                    post.source,
                    post.url
                ))

                if cursor.rowcount == 0:
                    return False  # Duplicate

                # Insert keywords
                for keyword in post.matched_keywords:
                    cursor.execute("""
                        INSERT INTO post_keywords (post_id, keyword)
                        VALUES (?, ?)
                    """, (post.post_id, keyword))

                # Insert categories
                for category in post.categories:
                    cursor.execute("""
                        INSERT INTO post_categories (post_id, category)
                        VALUES (?, ?)
                    """, (post.post_id, category))

                return True

        except Exception as e:
            logger.error(f"Error storing post {post.post_id}: {e}")
            return False

    def store_batch(self, posts: List[OSINTPost]) -> Dict[str, int]:
        """
        Store multiple posts.

        Args:
            posts: List of posts to store

        Returns:
            Dict with counts: {"stored": n, "duplicates": n, "errors": n}
        """
        results = {"stored": 0, "duplicates": 0, "errors": 0}

        for post in posts:
            try:
                if self.store(post):
                    results["stored"] += 1
                else:
                    results["duplicates"] += 1
            except Exception:
                results["errors"] += 1

        logger.info(
            f"Stored {results['stored']} posts, "
            f"{results['duplicates']} duplicates, "
            f"{results['errors']} errors"
        )
        return results

    def _row_to_post(self, conn, row) -> OSINTPost:
        """
        Reconstruct an OSINTPost from a database row plus its keywords/categories.

        Args:
            conn: Active database connection
            row: sqlite3.Row from the posts table

        Returns:
            OSINTPost instance
        """
        cursor = conn.cursor()
        post_id = row["post_id"]

        cursor.execute("SELECT keyword FROM post_keywords WHERE post_id = ?", (post_id,))
        keywords = [r[0] for r in cursor.fetchall()]

        cursor.execute("SELECT category FROM post_categories WHERE post_id = ?", (post_id,))
        categories = [r[0] for r in cursor.fetchall()]

        return OSINTPost(
            post_id=post_id,
            date=datetime.fromisoformat(row["date"]),
            username=row["username"],
            text=row["text"],
            source=row["source"],
            url=row["url"],
            matched_keywords=keywords,
            categories=categories
        )

    def get_by_id(self, post_id: str) -> Optional[OSINTPost]:
        """
        Retrieve a post by its ID.

        Args:
            post_id: The post ID

        Returns:
            OSINTPost or None if not found
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM posts WHERE post_id = ?", (post_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            return self._row_to_post(conn, row)

    def query_by_keyword(
        self,
        keyword: str,
        limit: int = 100,
        since: Optional[datetime] = None
    ) -> List[OSINTPost]:
        """
        Query posts containing a specific keyword.

        Args:
            keyword: Keyword to search for
            limit: Maximum results
            since: Only return posts after this date

        Returns:
            List of matching posts
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if since:
                cursor.execute(
                    "SELECT p.* FROM posts p "
                    "JOIN post_keywords pk ON p.post_id = pk.post_id "
                    "WHERE pk.keyword = ? AND p.date > ? "
                    "ORDER BY p.date DESC LIMIT ?",
                    (keyword, since.isoformat(), limit)
                )
            else:
                cursor.execute(
                    "SELECT p.* FROM posts p "
                    "JOIN post_keywords pk ON p.post_id = pk.post_id "
                    "WHERE pk.keyword = ? "
                    "ORDER BY p.date DESC LIMIT ?",
                    (keyword, limit)
                )
            return [self._row_to_post(conn, row) for row in cursor.fetchall()]

    def query_by_category(
        self,
        category: str,
        limit: int = 100,
        since: Optional[datetime] = None
    ) -> List[OSINTPost]:
        """
        Query posts in a specific category.

        Args:
            category: Category (e.g., "military/aviation")
            limit: Maximum results
            since: Only return posts after this date

        Returns:
            List of matching posts
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if since:
                cursor.execute(
                    "SELECT p.* FROM posts p "
                    "JOIN post_categories pc ON p.post_id = pc.post_id "
                    "WHERE pc.category = ? AND p.date > ? "
                    "ORDER BY p.date DESC LIMIT ?",
                    (category, since.isoformat(), limit)
                )
            else:
                cursor.execute(
                    "SELECT p.* FROM posts p "
                    "JOIN post_categories pc ON p.post_id = pc.post_id "
                    "WHERE pc.category = ? "
                    "ORDER BY p.date DESC LIMIT ?",
                    (category, limit)
                )
            return [self._row_to_post(conn, row) for row in cursor.fetchall()]

    def query_by_source(
        self,
        source: str,
        limit: int = 100,
        since: Optional[datetime] = None
    ) -> List[OSINTPost]:
        """
        Query posts from a specific source.

        Args:
            source: Source identifier (e.g., "reddit/r/military")
            limit: Maximum results
            since: Only return posts after this date

        Returns:
            List of matching posts
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if since:
                cursor.execute(
                    "SELECT * FROM posts WHERE source = ? AND date > ? "
                    "ORDER BY date DESC LIMIT ?",
                    (source, since.isoformat(), limit)
                )
            else:
                cursor.execute(
                    "SELECT * FROM posts WHERE source = ? "
                    "ORDER BY date DESC LIMIT ?",
                    (source, limit)
                )
            return [self._row_to_post(conn, row) for row in cursor.fetchall()]

    def get_keyword_counts(
        self,
        since: Optional[datetime] = None,
        limit: int = 50
    ) -> List[tuple]:
        """
        Get keyword frequency counts.

        Args:
            since: Only count posts after this date
            limit: Maximum keywords to return

        Returns:
            List of (keyword, count) tuples, sorted by count descending
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if since:
                cursor.execute(
                    "SELECT pk.keyword, COUNT(*) as cnt FROM post_keywords pk "
                    "JOIN posts p ON pk.post_id = p.post_id "
                    "WHERE p.date > ? "
                    "GROUP BY pk.keyword ORDER BY cnt DESC LIMIT ?",
                    (since.isoformat(), limit)
                )
            else:
                cursor.execute(
                    "SELECT keyword, COUNT(*) as cnt FROM post_keywords "
                    "GROUP BY keyword ORDER BY cnt DESC LIMIT ?",
                    (limit,)
                )
            return [(row[0], row[1]) for row in cursor.fetchall()]

    def get_category_counts(
        self,
        since: Optional[datetime] = None
    ) -> Dict[str, int]:
        """
        Get post counts by category.

        Args:
            since: Only count posts after this date

        Returns:
            Dict mapping category -> count
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if since:
                cursor.execute(
                    "SELECT pc.category, COUNT(*) as cnt FROM post_categories pc "
                    "JOIN posts p ON pc.post_id = p.post_id "
                    "WHERE p.date > ? "
                    "GROUP BY pc.category ORDER BY cnt DESC",
                    (since.isoformat(),)
                )
            else:
                cursor.execute(
                    "SELECT category, COUNT(*) as cnt FROM post_categories "
                    "GROUP BY category ORDER BY cnt DESC"
                )
            return {row[0]: row[1] for row in cursor.fetchall()}

    def get_hourly_counts(
        self,
        keyword: Optional[str] = None,
        category: Optional[str] = None,
        days: int = 7
    ) -> List[tuple]:
        """
        Get hourly post counts for time series analysis.

        Args:
            keyword: Filter by keyword (optional)
            category: Filter by category (optional)
            days: Number of days to look back

        Returns:
            List of (hour_timestamp, count) tuples
        """
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            if keyword:
                cursor.execute(
                    "SELECT strftime('%Y-%m-%d %H:00:00', p.date) as hour, COUNT(*) as cnt "
                    "FROM posts p "
                    "JOIN post_keywords pk ON p.post_id = pk.post_id "
                    "WHERE pk.keyword = ? AND p.date > ? "
                    "GROUP BY hour ORDER BY hour",
                    (keyword, cutoff)
                )
            elif category:
                cursor.execute(
                    "SELECT strftime('%Y-%m-%d %H:00:00', p.date) as hour, COUNT(*) as cnt "
                    "FROM posts p "
                    "JOIN post_categories pc ON p.post_id = pc.post_id "
                    "WHERE pc.category = ? AND p.date > ? "
                    "GROUP BY hour ORDER BY hour",
                    (category, cutoff)
                )
            else:
                cursor.execute(
                    "SELECT strftime('%Y-%m-%d %H:00:00', date) as hour, COUNT(*) as cnt "
                    "FROM posts WHERE date > ? "
                    "GROUP BY hour ORDER BY hour",
                    (cutoff,)
                )

            return [(row[0], row[1]) for row in cursor.fetchall()]

    def get_all_posts(
        self,
        limit: int = 10000,
        since: Optional[datetime] = None
    ) -> List[OSINTPost]:
        """
        Retrieve all posts, optionally filtered by date.

        Args:
            limit: Maximum number of posts to return
            since: Only return posts after this date

        Returns:
            List of OSINTPost objects
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if since:
                cursor.execute(
                    "SELECT * FROM posts WHERE date > ? ORDER BY date DESC LIMIT ?",
                    (since.isoformat(), limit)
                )
            else:
                cursor.execute(
                    "SELECT * FROM posts ORDER BY date DESC LIMIT ?",
                    (limit,)
                )
            return [self._row_to_post(conn, row) for row in cursor.fetchall()]

    def cleanup_old_posts(self) -> int:
        """
        Remove posts older than retention period.

        Returns:
            Number of posts deleted
        """
        if self.retention_days <= 0:
            return 0

        cutoff = datetime.utcnow() - timedelta(days=self.retention_days)

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get post IDs to delete
            cursor.execute(
                "SELECT post_id FROM posts WHERE date < ?",
                (cutoff.isoformat(),)
            )
            post_ids = [row[0] for row in cursor.fetchall()]

            if not post_ids:
                return 0

            # Delete from related tables first
            placeholders = ",".join("?" * len(post_ids))
            cursor.execute(
                f"DELETE FROM post_keywords WHERE post_id IN ({placeholders})",
                post_ids
            )
            cursor.execute(
                f"DELETE FROM post_categories WHERE post_id IN ({placeholders})",
                post_ids
            )
            cursor.execute(
                f"DELETE FROM posts WHERE post_id IN ({placeholders})",
                post_ids
            )

            logger.info(f"Cleaned up {len(post_ids)} old posts")
            return len(post_ids)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dict with various statistics
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            stats = {}

            cursor.execute("SELECT COUNT(*) FROM posts")
            stats["total_posts"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT source) FROM posts")
            stats["unique_sources"] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT keyword) FROM post_keywords")
            stats["unique_keywords_matched"] = cursor.fetchone()[0]

            cursor.execute("SELECT MIN(date), MAX(date) FROM posts")
            row = cursor.fetchone()
            stats["date_range"] = {"min": row[0], "max": row[1]}

            return stats


# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Example usage
    storage = Storage("test_osint.db")

    # Create a test post
    test_post = OSINTPost(
        post_id="test_123",
        date=datetime.now(),
        username="test_user",
        text="Saw military helicopters flying low over downtown.",
        source="reddit/r/test",
        url="https://reddit.com/r/test/123",
        matched_keywords=["military", "helicopters"],
        categories=["military/aviation"]
    )

    # Store it
    if storage.store(test_post):
        print("Post stored successfully")
    else:
        print("Post already exists (duplicate)")

    # Get stats
    stats = storage.get_stats()
    print(f"Total posts: {stats['total_posts']}")
