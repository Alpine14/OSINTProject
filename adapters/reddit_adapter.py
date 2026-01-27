"""
Reddit adapter for OSINT collection.

Uses PRAW (Python Reddit API Wrapper) to fetch posts from specified subreddits.
Reddit's API is well-documented and has generous rate limits for read-only access.

Setup:
    1. Create a Reddit account
    2. Go to https://www.reddit.com/prefs/apps
    3. Create a "script" type application
    4. Note your client_id and client_secret

PRAW Docs: https://praw.readthedocs.io/
"""

import logging
from datetime import datetime
from typing import List, Optional

# TODO: pip install praw
# import praw

from adapters.base import SourceAdapter
from models.post import OSINTPost
from config import REDDIT_SUBREDDITS

logger = logging.getLogger(__name__)


class RedditAdapter(SourceAdapter):
    """
    Fetches posts from Reddit using PRAW.

    Attributes:
        source_name: Identifier for this source ("reddit")
        subreddits: List of subreddit names to monitor
        reddit: PRAW Reddit instance
    """

    source_name = "reddit"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        user_agent: str = "OSINT Collector v1.0",
        subreddits: Optional[List[str]] = None
    ):
        """
        Initialize the Reddit adapter.

        Args:
            client_id: Reddit API client ID
            client_secret: Reddit API client secret
            user_agent: User agent string for API requests
            subreddits: List of subreddits to monitor (defaults to config)
        """
        self.subreddits = subreddits or REDDIT_SUBREDDITS
        self.reddit = None  # Will hold PRAW instance

        # TODO: Initialize PRAW client
        # self.reddit = praw.Reddit(
        #     client_id=client_id,
        #     client_secret=client_secret,
        #     user_agent=user_agent
        # )

    def fetch(self, limit_per_subreddit: int = 25) -> List[OSINTPost]:
        """
        Fetch recent posts from all configured subreddits.

        Args:
            limit_per_subreddit: Maximum posts to fetch per subreddit

        Returns:
            List of OSINTPost objects (keywords not yet matched)
        """
        posts = []

        for subreddit_name in self.subreddits:
            try:
                subreddit_posts = self._fetch_subreddit(subreddit_name, limit_per_subreddit)
                posts.extend(subreddit_posts)
                logger.info(f"Fetched {len(subreddit_posts)} posts from r/{subreddit_name}")
            except Exception as e:
                logger.error(f"Error fetching r/{subreddit_name}: {e}")
                continue

        return posts

    def _fetch_subreddit(self, subreddit_name: str, limit: int) -> List[OSINTPost]:
        """
        Fetch posts from a single subreddit.

        Args:
            subreddit_name: Name of the subreddit (without r/ prefix)
            limit: Maximum number of posts to fetch

        Returns:
            List of OSINTPost objects
        """
        posts = []

        # TODO: Implement PRAW fetching logic
        # subreddit = self.reddit.subreddit(subreddit_name)
        #
        # # Fetch from "new" to get most recent posts
        # for submission in subreddit.new(limit=limit):
        #     post = self._convert_submission(submission, subreddit_name)
        #     posts.append(post)

        return posts

    def _convert_submission(self, submission, subreddit_name: str) -> OSINTPost:
        """
        Convert a PRAW Submission object to an OSINTPost.

        Args:
            submission: PRAW Submission object
            subreddit_name: Name of the source subreddit

        Returns:
            OSINTPost object
        """
        # TODO: Implement conversion
        #
        # Combine title and selftext for full content
        # text = submission.title
        # if submission.selftext:
        #     text += "\n\n" + submission.selftext
        #
        # return OSINTPost(
        #     post_id=f"reddit_{submission.id}",
        #     date=datetime.utcfromtimestamp(submission.created_utc),
        #     username=str(submission.author) if submission.author else "[deleted]",
        #     text=text,
        #     source=f"reddit/r/{subreddit_name}",
        #     url=f"https://reddit.com{submission.permalink}",
        #     matched_keywords=[],  # Filled in later by KeywordMatcher
        #     categories=[]         # Filled in later by KeywordMatcher
        # )

        pass

    def fetch_comments(self, post_id: str, limit: int = 50) -> List[OSINTPost]:
        """
        Fetch comments for a specific post.

        This is useful for deeper analysis of high-signal posts.

        Args:
            post_id: Reddit submission ID (without reddit_ prefix)
            limit: Maximum comments to fetch

        Returns:
            List of OSINTPost objects representing comments
        """
        comments = []

        # TODO: Implement comment fetching
        # submission = self.reddit.submission(id=post_id)
        # submission.comments.replace_more(limit=0)  # Don't fetch "more comments"
        #
        # for comment in submission.comments.list()[:limit]:
        #     if comment.body == "[deleted]":
        #         continue
        #     comments.append(self._convert_comment(comment, post_id))

        return comments

    def _convert_comment(self, comment, parent_post_id: str) -> OSINTPost:
        """
        Convert a PRAW Comment object to an OSINTPost.

        Args:
            comment: PRAW Comment object
            parent_post_id: ID of the parent submission

        Returns:
            OSINTPost object
        """
        # TODO: Implement conversion
        pass


# =============================================================================
# USAGE EXAMPLE
# =============================================================================

if __name__ == "__main__":
    # Example usage - won't work until you add credentials and uncomment PRAW code
    adapter = RedditAdapter(
        client_id="YOUR_CLIENT_ID",
        client_secret="YOUR_CLIENT_SECRET"
    )

    posts = adapter.fetch(limit_per_subreddit=10)
    print(f"Fetched {len(posts)} total posts")

    for post in posts[:5]:
        print(f"- [{post.source}] {post.text[:100]}...")
