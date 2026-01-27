"""
Keyword matching module for OSINT collection.

This module matches collected posts against the keyword database
and assigns domain/subdomain categories based on matches.

Features:
    - Efficient regex-based matching
    - Multi-keyword matching (posts can match multiple categories)
    - Domain and subdomain categorization
    - Match scoring based on keyword frequency
"""

import logging
import re
from typing import List, Dict, Set, Tuple
from dataclasses import dataclass

from models.post import OSINTPost
from processing.keyword_loader import KeywordLoader

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of keyword matching for a single post."""
    matched_keywords: List[str]  # List of keywords that matched
    domains: Set[str]            # Set of domains matched
    subdomains: Set[str]         # Set of subdomains matched
    match_count: int             # Total number of keyword matches
    relevance_score: float       # Normalized relevance score (0.0 - 1.0)


class KeywordMatcher:
    """
    Matches posts against the keyword database.

    Uses precompiled regex patterns for efficient matching across
    large volumes of posts.

    Attributes:
        keywords: Nested dict of domain -> subdomain -> [keywords]
        patterns: Precompiled regex patterns for each keyword
    """

    def __init__(self, keywords_path: str = "data/Keywords.csv"):
        """
        Initialize the keyword matcher.

        Args:
            keywords_path: Path to the data/Keywords.csv file
        """
        self.loader = KeywordLoader(keywords_path)
        self.keywords = {}  # domain -> subdomain -> [keywords]
        self.keyword_to_category = {}  # keyword -> (domain, subdomain)
        self._pattern = None  # Combined regex pattern

        self._load_keywords()

    def _load_keywords(self):
        """
        Load and index keywords from the CSV file.

        Builds:
            - Nested dict for category lookup
            - Reverse index from keyword to category
            - Combined regex pattern for matching
        """
        # TODO: Implement keyword loading with new format
        #
        # raw_data = self.loader.load()  # Returns nested dict
        #
        # all_keywords = []
        #
        # for domain, subdomains in raw_data.items():
        #     if domain not in self.keywords:
        #         self.keywords[domain] = {}
        #
        #     for subdomain, keywords in subdomains.items():
        #         self.keywords[domain][subdomain] = keywords
        #
        #         for keyword in keywords:
        #             self.keyword_to_category[keyword.lower()] = (domain, subdomain)
        #             all_keywords.append(keyword)
        #
        # # Compile combined pattern
        # self._pattern = self._compile_pattern(all_keywords)
        #
        # logger.info(
        #     f"Loaded {len(all_keywords)} keywords across "
        #     f"{len(self.keywords)} domains"
        # )

        pass

    def _compile_pattern(self, keywords: List[str]) -> re.Pattern:
        """
        Compile all keywords into a single regex pattern.

        Using a single pattern is more efficient than checking
        each keyword individually.

        Args:
            keywords: List of all keywords

        Returns:
            Compiled regex pattern
        """
        # TODO: Implement pattern compilation
        #
        # # Sort by length (longest first) to ensure longer phrases match first
        # sorted_keywords = sorted(keywords, key=len, reverse=True)
        #
        # # Escape special regex characters
        # escaped = [re.escape(kw) for kw in sorted_keywords]
        #
        # # Join with OR and add word boundaries
        # pattern = r'\b(' + '|'.join(escaped) + r')\b'
        #
        # return re.compile(pattern, re.IGNORECASE)

        pass

    def match(self, post: OSINTPost) -> MatchResult:
        """
        Match a post against all keywords.

        Args:
            post: The post to match

        Returns:
            MatchResult with all matches and categories
        """
        matched_keywords = []
        domains = set()
        subdomains = set()

        # TODO: Implement matching
        #
        # if not self._pattern:
        #     return MatchResult([], set(), set(), 0, 0.0)
        #
        # text_lower = post.text.lower()
        #
        # # Find all matches
        # for match in self._pattern.finditer(text_lower):
        #     keyword = match.group().lower()
        #     matched_keywords.append(keyword)
        #
        #     # Look up category
        #     if keyword in self.keyword_to_category:
        #         domain, subdomain = self.keyword_to_category[keyword]
        #         domains.add(domain)
        #         subdomains.add(f"{domain}/{subdomain}")
        #
        # # Calculate relevance score
        # # Simple: ratio of matched keywords to total words
        # word_count = len(text_lower.split())
        # relevance = min(1.0, len(matched_keywords) / max(1, word_count) * 10)
        #
        # return MatchResult(
        #     matched_keywords=matched_keywords,
        #     domains=domains,
        #     subdomains=subdomains,
        #     match_count=len(matched_keywords),
        #     relevance_score=relevance
        # )

        return MatchResult([], set(), set(), 0, 0.0)

    def match_and_update(self, post: OSINTPost) -> OSINTPost:
        """
        Match a post and update its keyword/category fields in place.

        Args:
            post: The post to match and update

        Returns:
            The same post object with updated fields
        """
        result = self.match(post)

        post.matched_keywords = result.matched_keywords
        post.categories = list(result.subdomains)

        return post

    def match_batch(self, posts: List[OSINTPost]) -> Tuple[List[OSINTPost], Dict]:
        """
        Match a batch of posts and return statistics.

        Args:
            posts: List of posts to match

        Returns:
            Tuple of (updated_posts, statistics_dict)
        """
        stats = {
            "total_posts": len(posts),
            "posts_with_matches": 0,
            "total_matches": 0,
            "domain_counts": {},
            "subdomain_counts": {},
        }

        updated_posts = []

        for post in posts:
            result = self.match(post)

            # Update post
            post.matched_keywords = result.matched_keywords
            post.categories = list(result.subdomains)
            updated_posts.append(post)

            # Update stats
            if result.match_count > 0:
                stats["posts_with_matches"] += 1
                stats["total_matches"] += result.match_count

                for domain in result.domains:
                    stats["domain_counts"][domain] = stats["domain_counts"].get(domain, 0) + 1

                for subdomain in result.subdomains:
                    stats["subdomain_counts"][subdomain] = stats["subdomain_counts"].get(subdomain, 0) + 1

        return updated_posts, stats

    def get_keywords_for_domain(self, domain: str) -> List[str]:
        """
        Get all keywords for a specific domain.

        Args:
            domain: Domain name (e.g., "military")

        Returns:
            List of all keywords in that domain
        """
        keywords = []

        if domain in self.keywords:
            for subdomain_keywords in self.keywords[domain].values():
                keywords.extend(subdomain_keywords)

        return keywords

    def get_keywords_for_subdomain(self, domain: str, subdomain: str) -> List[str]:
        """
        Get all keywords for a specific subdomain.

        Args:
            domain: Domain name
            subdomain: Subdomain name

        Returns:
            List of keywords in that subdomain
        """
        if domain in self.keywords and subdomain in self.keywords[domain]:
            return self.keywords[domain][subdomain]
        return []


# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Example usage
    from datetime import datetime

    matcher = KeywordMatcher("data/Keywords.csv")

    # Test with sample text
    test_post = OSINTPost(
        post_id="test_1",
        date=datetime.now(),
        username="test_user",
        text="Saw a military convoy and blackhawk helicopter near the airport. Runway closed.",
        source="test",
        url="http://example.com",
        matched_keywords=[],
        categories=[]
    )

    result = matcher.match(test_post)
    print(f"Matched keywords: {result.matched_keywords}")
    print(f"Domains: {result.domains}")
    print(f"Subdomains: {result.subdomains}")
    print(f"Relevance score: {result.relevance_score:.2f}")
