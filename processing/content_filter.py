"""
Content filtering module for OSINT collection.

This module provides multiple layers of content filtering to ensure
collected data is appropriate for analysis and demonstration.

Filtering Layers:
    1. Blocklist - Fast, local filtering of known bad terms
    2. Perspective API - ML-based toxicity scoring (optional)
    3. Sentiment flagging - Mark posts with detected bias

The goal is to filter out noise while retaining analytical value.
"""

import logging
import re
from dataclasses import dataclass
from typing import List, Set, Tuple, Optional
from pathlib import Path

import requests

from models.post import OSINTPost
from config import filter_config

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    """Result of content filtering."""
    passed: bool
    reason: Optional[str] = None  # Why it was filtered (if applicable)
    toxicity_score: Optional[float] = None
    sentiment_flags: List[str] = None  # Detected sentiment categories

    def __post_init__(self):
        if self.sentiment_flags is None:
            self.sentiment_flags = []


class ContentFilter:
    """
    Multi-layer content filter for OSINT posts.

    Combines blocklist filtering with optional ML-based moderation
    to filter inappropriate content while preserving analytical value.

    Attributes:
        blocklist: Set of blocked terms (lowercase)
        perspective_api_key: API key for Google Perspective (optional)
        toxicity_threshold: Score above which posts are filtered
    """

    def __init__(
        self,
        blocklist_path: Optional[str] = None,
        perspective_api_key: Optional[str] = None,
        toxicity_threshold: float = 0.7
    ):
        """
        Initialize the content filter.

        Args:
            blocklist_path: Path to blocklist file (one term per line)
            perspective_api_key: Google Perspective API key (optional)
            toxicity_threshold: Toxicity score threshold (0.0 - 1.0)
        """
        self.blocklist = self._load_blocklist(blocklist_path or filter_config.blocklist_path)
        self.perspective_api_key = perspective_api_key or filter_config.perspective_api_key
        self.toxicity_threshold = toxicity_threshold

        # Precompile regex for blocklist matching (word boundaries)
        self._blocklist_pattern = self._compile_blocklist_pattern()

    def _load_blocklist(self, path: str) -> Set[str]:
        """
        Load blocklist from file.

        Args:
            path: Path to blocklist file

        Returns:
            Set of blocked terms (lowercase)
        """
        blocklist = set()

        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    term = line.strip().lower()
                    if term and not term.startswith("#"):  # Skip comments
                        blocklist.add(term)
            logger.info(f"Loaded {len(blocklist)} terms from blocklist")
        except FileNotFoundError:
            logger.warning(f"Blocklist file not found: {path}. Using default blocklist.")
            blocklist = self._get_default_blocklist()

        return blocklist

    def _get_default_blocklist(self) -> Set[str]:
        """
        Return a minimal default blocklist.

        This is a fallback if no blocklist file exists.
        Focuses on the most egregious terms to catch.

        Returns:
            Set of blocked terms
        """
        # TODO: Add your default blocklist terms here
        # These should be terms that are NEVER acceptable in your output
        #
        # Example (you should expand this):
        # return {
        #     "n-word-here",  # Obviously use the actual terms
        #     "other-slurs",
        #     # ... etc
        # }

        return set()

    def _compile_blocklist_pattern(self) -> Optional[re.Pattern]:
        """
        Compile blocklist into a single regex pattern for efficient matching.

        Uses word boundaries to avoid false positives (e.g., "analysis"
        shouldn't match "anal").

        Returns:
            Compiled regex pattern, or None if blocklist is empty
        """
        if not self.blocklist:
            return None

        # Escape special regex characters in each term
        escaped_terms = [re.escape(term) for term in self.blocklist]

        # Join with OR and add word boundaries
        pattern = r'\b(' + '|'.join(escaped_terms) + r')\b'

        return re.compile(pattern, re.IGNORECASE)

    def filter(self, post: OSINTPost) -> FilterResult:
        """
        Apply all filtering layers to a post.

        Args:
            post: The post to filter

        Returns:
            FilterResult indicating whether the post passed
        """
        # Layer 1: Blocklist check (fast, local)
        blocklist_result = self._check_blocklist(post.text)
        if not blocklist_result.passed:
            if filter_config.log_filtered:
                logger.debug(f"Filtered by blocklist: {post.post_id}")
            return blocklist_result

        # Layer 2: Perspective API check (if configured)
        if self.perspective_api_key:
            perspective_result = self._check_perspective(post.text)
            if not perspective_result.passed:
                if filter_config.log_filtered:
                    logger.debug(
                        f"Filtered by Perspective (toxicity={perspective_result.toxicity_score}): "
                        f"{post.post_id}"
                    )
                return perspective_result

        # Post passed all filters
        return FilterResult(passed=True)

    def _check_blocklist(self, text: str) -> FilterResult:
        """
        Check text against blocklist.

        Args:
            text: Text to check

        Returns:
            FilterResult
        """
        if not self._blocklist_pattern:
            return FilterResult(passed=True)

        match = self._blocklist_pattern.search(text)
        if match:
            return FilterResult(
                passed=False,
                reason=f"blocklist_match:{match.group()}"
            )

        return FilterResult(passed=True)

    def _check_perspective(self, text: str) -> FilterResult:
        """
        Check text using Google Perspective API.

        Perspective API analyzes text for various toxicity attributes:
        - TOXICITY: Rude, disrespectful, or unreasonable comment
        - SEVERE_TOXICITY: Very hateful, aggressive, or disrespectful
        - IDENTITY_ATTACK: Negative or hateful against an identity group
        - INSULT: Insulting or inflammatory language
        - PROFANITY: Swear words, curse words, or other obscene language
        - THREAT: Intention to inflict pain, injury, or violence

        API Docs: https://developers.perspectiveapi.com/

        Args:
            text: Text to analyze

        Returns:
            FilterResult with toxicity score
        """
        url = "https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze"
        params = {"key": self.perspective_api_key}

        payload = {
            "comment": {"text": text},
            "languages": ["en"],
            "requestedAttributes": {
                "TOXICITY": {},
                "SEVERE_TOXICITY": {},
                "IDENTITY_ATTACK": {},
            }
        }

        try:
            response = requests.post(url, params=params, json=payload)
            response.raise_for_status()
            result = response.json()

            # Extract scores
            toxicity = result["attributeScores"]["TOXICITY"]["summaryScore"]["value"]
            identity_attack = result["attributeScores"]["IDENTITY_ATTACK"]["summaryScore"]["value"]

            # Check thresholds
            if toxicity > self.toxicity_threshold:
                return FilterResult(
                    passed=False,
                    reason=f"toxicity:{toxicity:.2f}",
                    toxicity_score=toxicity
                )

            if identity_attack > filter_config.identity_attack_threshold:
                return FilterResult(
                    passed=False,
                    reason=f"identity_attack:{identity_attack:.2f}",
                    toxicity_score=toxicity
                )

            return FilterResult(passed=True, toxicity_score=toxicity)

        except Exception as e:
            logger.error(f"Perspective API error: {e}")
            # Fail open - if API fails, let the post through
            return FilterResult(passed=True)

    def filter_batch(self, posts: List[OSINTPost]) -> Tuple[List[OSINTPost], int]:
        """
        Filter a batch of posts.

        Args:
            posts: List of posts to filter

        Returns:
            Tuple of (passed_posts, filtered_count)
        """
        passed = []
        filtered_count = 0

        for post in posts:
            result = self.filter(post)
            if result.passed:
                passed.append(post)
            else:
                filtered_count += 1

        logger.info(f"Filtered {filtered_count}/{len(posts)} posts")
        return passed, filtered_count


# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Example usage
    filter = ContentFilter()

    # Test with sample text
    from datetime import datetime

    test_post = OSINTPost(
        post_id="test_1",
        date=datetime.now(),
        username="test_user",
        text="This is a normal post about emergency response activities.",
        source="test",
        url="http://example.com",
        matched_keywords=[],
        categories=[]
    )

    result = filter.filter(test_post)
    print(f"Post passed: {result.passed}")
    if not result.passed:
        print(f"Reason: {result.reason}")
