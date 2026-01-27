"""
Data model for OSINT posts.

This module defines the core data structure used throughout the project.
All collected posts are normalized to this format regardless of source.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List


@dataclass
class OSINTPost:
    """
    Represents a single post collected from an OSINT source.

    This is the universal data format used throughout the pipeline:
        Source Adapter -> Filter -> Matcher -> Storage

    Attributes:
        post_id: Unique identifier (format: "{source}_{native_id}")
        date: When the post was created (UTC)
        username: Author's username (or "Anonymous" for anonymous platforms)
        text: Full text content of the post
        source: Source identifier (e.g., "reddit/r/news", "4chan/news")
        url: Direct link to the original post
        matched_keywords: Keywords from our database found in this post
        categories: Domain/subdomain categories matched (e.g., "military/aviation")

    Example:
        post = OSINTPost(
            post_id="reddit_abc123",
            date=datetime.utcnow(),
            username="example_user",
            text="Saw multiple military helicopters flying over downtown...",
            source="reddit/r/military",
            url="https://reddit.com/r/military/comments/abc123",
            matched_keywords=["military", "helicopters"],
            categories=["military/aviation"]
        )
    """

    post_id: str
    date: datetime
    username: str
    text: str
    source: str
    url: str
    matched_keywords: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        """Return a brief string representation."""
        preview = self.text[:50] + "..." if len(self.text) > 50 else self.text
        return f"[{self.source}] {preview}"

    def __repr__(self) -> str:
        """Return a detailed string representation."""
        return (
            f"OSINTPost(post_id={self.post_id!r}, "
            f"source={self.source!r}, "
            f"keywords={len(self.matched_keywords)})"
        )

    def to_dict(self) -> dict:
        """
        Convert to dictionary for serialization.

        Returns:
            Dict representation of the post
        """
        return {
            "post_id": self.post_id,
            "date": self.date.isoformat(),
            "username": self.username,
            "text": self.text,
            "source": self.source,
            "url": self.url,
            "matched_keywords": self.matched_keywords,
            "categories": self.categories
        }

    @classmethod
    def from_dict(cls, data: dict) -> "OSINTPost":
        """
        Create an OSINTPost from a dictionary.

        Args:
            data: Dict with post data

        Returns:
            OSINTPost instance
        """
        return cls(
            post_id=data["post_id"],
            date=datetime.fromisoformat(data["date"]),
            username=data["username"],
            text=data["text"],
            source=data["source"],
            url=data["url"],
            matched_keywords=data.get("matched_keywords", []),
            categories=data.get("categories", [])
        )
