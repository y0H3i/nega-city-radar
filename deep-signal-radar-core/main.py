from __future__ import annotations

import json
import sys
from typing import List

from dotenv import load_dotenv

from analyzer import LLMAnalysisError, SignalItem, analyze_articles
from scraper import RSSFetchError, fetch_latest_articles


def run_pipeline() -> int:
    """
    Run the end-to-end pipeline:
    - Fetch latest Hacker News articles via RSS
    - Analyze them with an LLM to extract signal scores
    - Filter by score >= 7
    - Pretty-print the resulting JSON to stdout

    Returns:
        Exit code (0 on success, non-zero on failure).
    """
    try:
        articles = fetch_latest_articles()
    except RSSFetchError as exc:
        print(f"[Error] Failed to fetch RSS feed: {exc}", file=sys.stderr)
        return 1

    if not articles:
        print("[Info] No articles found in the RSS feed.", file=sys.stderr)
        return 0

    try:
        signals: List[SignalItem] = analyze_articles(articles)
    except LLMAnalysisError as exc:
        print(f"[Error] Failed to analyze articles with LLM: {exc}", file=sys.stderr)
        return 1

    filtered = [s.model_dump() for s in signals if s.signal_score >= 7]

    if not filtered:
        print("[Info] No strong signals (score >= 7) were found.", file=sys.stderr)
        return 0

    print(json.dumps(filtered, indent=2, ensure_ascii=False))
    return 0


def main() -> None:
    """Entry point for the CLI."""
    # Load environment variables from a .env file if present.
    load_dotenv()
    raise_code = run_pipeline()
    raise SystemExit(raise_code)


if __name__ == "__main__":
    main()

