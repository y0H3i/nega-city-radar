from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

import feedparser
import requests


DEFAULT_HN_RSS_URL = "https://news.ycombinator.com/rss"


@dataclass(slots=True)
class Article:
    """Represents a single article item fetched from an RSS feed."""

    title: str
    url: str


class RSSFetchError(Exception):
    """Raised when an RSS feed cannot be fetched or parsed."""


def fetch_rss_feed(
    url: str = DEFAULT_HN_RSS_URL,
    timeout: float = 10.0,
) -> str:
    """
    Fetch the raw RSS XML from the given URL.

    Args:
        url: RSS feed URL.
        timeout: Request timeout in seconds.

    Returns:
        Raw RSS XML as a string.

    Raises:
        RSSFetchError: If the HTTP request fails or returns an invalid response.
    """
    try:
        response = requests.get(url, timeout=timeout)
    except requests.RequestException as exc:  # type: ignore[reportUnknownVariableType]
        raise RSSFetchError(f"Failed to fetch RSS feed: {exc}") from exc

    if not response.ok:
        raise RSSFetchError(f"RSS feed returned status code {response.status_code}")

    response.encoding = response.encoding or "utf-8"
    return response.text


def parse_rss(xml: str) -> List[Article]:
    """
    Parse the RSS XML into a list of Article objects.

    Args:
        xml: Raw RSS XML as a string.

    Returns:
        List of Article instances.

    Raises:
        RSSFetchError: If the XML cannot be parsed.
    """
    try:
        parsed = feedparser.parse(xml)
    except Exception as exc:  # pragma: no cover - very unlikely branch
        raise RSSFetchError(f"Failed to parse RSS XML: {exc}") from exc

    if parsed.bozo:
        # feedparser sets bozo when parsing failed or is suspicious
        raise RSSFetchError(f"Invalid RSS feed: {parsed.bozo_exception}")

    articles: List[Article] = []
    for entry in parsed.entries:
        title = getattr(entry, "title", "").strip()
        link = getattr(entry, "link", "").strip()
        if not title or not link:
            continue
        articles.append(Article(title=title, url=link))

    return articles


def fetch_latest_articles(
    url: str = DEFAULT_HN_RSS_URL,
    timeout: float = 10.0,
) -> List[Article]:
    """
    Convenience helper to fetch and parse the latest articles from an RSS feed.

    Args:
        url: RSS feed URL.
        timeout: Request timeout in seconds.

    Returns:
        List of Article instances.
    """
    xml = fetch_rss_feed(url=url, timeout=timeout)
    return parse_rss(xml)


def format_articles_for_prompt(articles: Iterable[Article]) -> str:
    """
    Format articles into a text block suitable for an LLM prompt.

    Args:
        articles: Iterable of Article instances.

    Returns:
        A newline-separated string representation of all articles.
    """
    lines: List[str] = []
    for idx, article in enumerate(articles, start=1):
        lines.append(f"{idx}. Title: {article.title}\n   URL: {article.url}")
    return "\n".join(lines)

