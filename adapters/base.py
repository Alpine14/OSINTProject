"""
Base adapter interface for OSINT sources.

All source adapters (Reddit, 4chan, etc.) must implement this interface.
This allows the collector to treat all sources uniformly.
"""

from abc import ABC, abstractmethod
from typing import List

from models.post import OSINTPost


class SourceAdapter(ABC):
    """
    Abstract base class for OSINT source adapters.

    Each adapter is responsible for:
        1. Connecting to its data source
        2. Fetching posts in a standardized format
        3. Handling rate limiting and errors
        4. Converting source-specific data to OSINTPost objects

    Attributes:
        source_name: Unique identifier for this source (e.g., "reddit", "4chan")
    """

    source_name: str

    @abstractmethod
    def fetch(self, **kwargs) -> List[OSINTPost]:
        """
        Fetch posts from this source.

        Args:
            **kwargs: Source-specific parameters (e.g., limit, subreddits)

        Returns:
            List of OSINTPost objects. Posts should have:
                - post_id: Unique identifier (include source prefix)
                - date: When the post was created
                - username: Author (or "Anonymous" for anonymous sources)
                - text: Post content
                - source: Source identifier (e.g., "reddit/r/news")
                - url: Link to original post
                - matched_keywords: Empty list (filled by KeywordMatcher)
                - categories: Empty list (filled by KeywordMatcher)
        """
        pass
