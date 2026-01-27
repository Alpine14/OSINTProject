"""
Source adapters for OSINT collection.

Each adapter implements the SourceAdapter interface and handles
fetching data from a specific platform.
"""

from adapters.base import SourceAdapter
from adapters.reddit_adapter import RedditAdapter
from adapters.chan_adapter import ChanAdapter

__all__ = ["SourceAdapter", "RedditAdapter", "ChanAdapter"]
