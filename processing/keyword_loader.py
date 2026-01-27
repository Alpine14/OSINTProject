"""
Keyword loader module for OSINT collection.

Loads keywords from CSV files and provides them in a structured format
for use by the KeywordMatcher.

Supports the hierarchical format: domain,subdomain,keyword
"""

import logging
from collections import defaultdict
from typing import Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


class KeywordLoader:
    """
    Loads keywords from CSV files.

    Supports the hierarchical keyword format:
        domain,subdomain,keyword

    Example:
        military,aviation,blackhawk
        military,aviation,helicopter
        military,ground,convoy
    """

    def __init__(self, path: str):
        """
        Initialize the keyword loader.

        Args:
            path: Path to the Keywords.csv file
        """
        self.path = path

    def load(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Load keywords from CSV into a nested dictionary.

        Returns:
            Nested dict: domain -> subdomain -> [keywords]

        Example:
            {
                "military": {
                    "aviation": ["blackhawk", "helicopter", ...],
                    "ground": ["convoy", "checkpoint", ...]
                },
                "emergency": {
                    "response": ["first responders", ...],
                    ...
                }
            }
        """
        try:
            df = pd.read_csv(self.path)
        except FileNotFoundError:
            logger.error(f"Keywords file not found: {self.path}")
            return {}
        except Exception as e:
            logger.error(f"Error loading keywords: {e}")
            return {}

        # Build nested structure
        result = defaultdict(lambda: defaultdict(list))

        for _, row in df.iterrows():
            domain = row["domain"].strip().lower()
            subdomain = row["subdomain"].strip().lower()
            keyword = row["keyword"].strip().lower()

            result[domain][subdomain].append(keyword)

        # Convert defaultdicts to regular dicts for cleaner output
        return {
            domain: dict(subdomains)
            for domain, subdomains in result.items()
        }

    def load_flat(self) -> Dict[str, List[str]]:
        """
        Load keywords into a flat structure (subdomain -> keywords).

        This is useful when you don't need domain-level grouping.

        Returns:
            Dict: "domain/subdomain" -> [keywords]

        Example:
            {
                "military/aviation": ["blackhawk", "helicopter", ...],
                "military/ground": ["convoy", ...],
                ...
            }
        """
        nested = self.load()
        flat = {}

        for domain, subdomains in nested.items():
            for subdomain, keywords in subdomains.items():
                key = f"{domain}/{subdomain}"
                flat[key] = keywords

        return flat

    def get_all_keywords(self) -> List[str]:
        """
        Get a flat list of all keywords.

        Returns:
            List of all keywords (may contain duplicates if keywords
            appear in multiple categories)
        """
        all_keywords = []
        nested = self.load()

        for subdomains in nested.values():
            for keywords in subdomains.values():
                all_keywords.extend(keywords)

        return all_keywords

    def get_domains(self) -> List[str]:
        """
        Get list of all domain names.

        Returns:
            List of domain names
        """
        return list(self.load().keys())

    def get_subdomains(self, domain: str) -> List[str]:
        """
        Get list of subdomain names for a specific domain.

        Args:
            domain: Domain name

        Returns:
            List of subdomain names
        """
        nested = self.load()
        if domain in nested:
            return list(nested[domain].keys())
        return []


class SentimentKeywordLoader:
    """
    Loads sentiment keywords from CSV.

    Sentiment keywords have the format:
        sentiment,intensity,keyword

    These are used for bias detection and post flagging.
    """

    def __init__(self, path: str = "data/sentiment_keywords.csv"):
        """
        Initialize the sentiment keyword loader.

        Args:
            path: Path to sentiment_keywords.csv
        """
        self.path = path

    def load(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Load sentiment keywords.

        Returns:
            Nested dict: sentiment -> intensity -> [keywords]

        Example:
            {
                "anti_authority": {
                    "mild": ["authoritarian", ...],
                    "strong": ["fascists", ...]
                },
                ...
            }
        """
        try:
            df = pd.read_csv(self.path)
        except FileNotFoundError:
            logger.warning(f"Sentiment keywords file not found: {self.path}")
            return {}

        result = defaultdict(lambda: defaultdict(list))

        for _, row in df.iterrows():
            sentiment = row["sentiment"].strip().lower()
            intensity = row["intensity"].strip().lower()
            keyword = row["keyword"].strip().lower()

            result[sentiment][intensity].append(keyword)

        return {
            sentiment: dict(intensities)
            for sentiment, intensities in result.items()
        }

    def get_all_by_intensity(self, intensity: str) -> List[str]:
        """
        Get all keywords of a specific intensity level.

        Args:
            intensity: "mild", "moderate", or "strong"

        Returns:
            List of keywords at that intensity
        """
        keywords = []
        data = self.load()

        for sentiment_data in data.values():
            if intensity in sentiment_data:
                keywords.extend(sentiment_data[intensity])

        return keywords


# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Test the keyword loader
    loader = KeywordLoader("data/Keywords.csv")

    print("=== Domains ===")
    for domain in loader.get_domains():
        print(f"  {domain}")

    print("\n=== Military Subdomains ===")
    for subdomain in loader.get_subdomains("military"):
        print(f"  {subdomain}")

    print("\n=== Sample Keywords (military/aviation) ===")
    nested = loader.load()
    if "military" in nested and "aviation" in nested["military"]:
        for kw in nested["military"]["aviation"][:5]:
            print(f"  {kw}")

    print(f"\n=== Total Keywords: {len(loader.get_all_keywords())} ===")

    # Test sentiment loader
    print("\n=== Sentiment Keywords ===")
    sentiment_loader = SentimentKeywordLoader()
    sentiment_data = sentiment_loader.load()
    for sentiment, intensities in sentiment_data.items():
        total = sum(len(kws) for kws in intensities.values())
        print(f"  {sentiment}: {total} keywords")
