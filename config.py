"""
Configuration settings for the OSINT collector.

This module centralizes all configuration to make the project easy to customize
and to keep sensitive values (API keys) separate from code logic.
"""

from dataclasses import dataclass, field
from typing import List, Optional

# =============================================================================
# 4CHAN CONFIGURATION
# =============================================================================

# SFW boards only - these have baseline moderation and won't show explicit content
CHAN_SFW_BOARDS = [
    "news",  # Current News
    "n",     # Transportation
]


# =============================================================================
# REDDIT CONFIGURATION
# =============================================================================

# Subreddits to monitor - curated for OSINT-relevant content
REDDIT_SUBREDDITS = [
    # Military/Defense
    "military",
    "army",
    "aviation",
    "flightradar24",

    # News/Current Events
    "news",
    "worldnews",
    "breakingnews",

    # Regional/Local (high signal for local events)
    "washingtondc",
    "chicago",
    "losangeles",
    "nyc",

    # Specific Topics
    "publicsafety",
    "emergencymanagement",
    "tropicalweather",
    "earthquakes",
]


# =============================================================================
# FILTERING CONFIGURATION
# =============================================================================

@dataclass
class FilterConfig:
    """Settings for content filtering."""

    # Anthropic/OpenAI moderation API (optional - enhances filtering)
    # Set to None to use blocklist-only filtering
    perspective_api_key: str = None

    # Toxicity threshold (0.0 - 1.0) - posts above this are discarded
    # 0.7 is a reasonable default that catches most slurs while keeping heated discussion
    toxicity_threshold: float = 0.7

    # Identity attack threshold - lower than toxicity to be more sensitive
    identity_attack_threshold: float = 0.5

    # Path to blocklist file (one term per line)
    blocklist_path: str = "data/blocklist.txt"

    # Whether to log filtered posts (useful for tuning, disable for production)
    log_filtered: bool = True


# =============================================================================
# STORAGE CONFIGURATION
# =============================================================================

@dataclass
class StorageConfig:
    """Settings for data storage."""

    # SQLite database path
    database_path: str = "osint_data.db"

    # How long to retain posts (days) - set to 0 for indefinite
    retention_days: int = 30


# =============================================================================
# COLLECTION CONFIGURATION
# =============================================================================

@dataclass
class CollectionConfig:
    """Settings for the collection process."""

    # How often to poll sources (seconds)
    poll_interval: int = 300  # 5 minutes

    # Maximum posts to fetch per source per poll
    max_posts_per_poll: int = 100

    # Whether to run continuously or single-shot
    continuous: bool = True

    # Sources to enable
    enable_reddit: bool = True
    enable_4chan: bool = True


# =============================================================================
# INSTANTIATE DEFAULT CONFIGS
# =============================================================================

filter_config = FilterConfig()
storage_config = StorageConfig()
collection_config = CollectionConfig()
