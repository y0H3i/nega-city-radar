from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import List, Literal, Optional, Sequence

import google.generativeai as genai
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError

from scraper import Article, format_articles_for_prompt

# Default wall-clock budget for a single batched LLM call (many RSS items can exceed 30s).
DEFAULT_ANALYSIS_TIMEOUT_SECONDS: float = 120.0

LLMProvider = Literal["gemini", "openai"]


class SignalItem(BaseModel):
    """Represents a single extracted signal from an article."""

    title: str = Field(..., description="Original article title.")
    url: str = Field(..., description="Original article URL.")
    signal_score: int = Field(
        ...,
        ge=1,
        le=10,
        description="Importance score (1-10) regarding AI, distributed systems, or new architectures.",
    )
    reason: str = Field(..., description="Explanation for the assigned signal score.")
    summary: str = Field(..., description="Short summary of the article's key idea.")


class LLMAnalysisError(Exception):
    """Raised when the LLM analysis fails or returns invalid data."""


def _get_llm_provider() -> LLMProvider:
    provider_raw = os.getenv("LLM_PROVIDER", "gemini").strip().lower()
    if provider_raw not in {"gemini", "openai"}:
        return "gemini"
    return provider_raw  # type: ignore[return-value]


def _build_prompt(articles: Sequence[Article]) -> str:
    articles_block = format_articles_for_prompt(articles)
    instruction = """
You are a system that evaluates news articles and extracts only the most valuable AI, distributed systems, or new architecture related signals.

You will be given a list of articles (title and URL). For each article, you MUST:
- Evaluate how important it is as a signal about:
  - Artificial Intelligence (AI, ML, LLMs, agents, tooling, etc.), OR
  - Distributed systems, scalability, reliability, cloud-native infrastructure, OR
  - Novel software/system architectures or paradigms.
- Assign a "signal_score" from 1 to 10 (10 = extremely important signal, 1 = not important).
- Provide a short "summary" of the main idea.
- Provide a "reason" explaining why you assigned that score.

Return your answer as a STRICT JSON array, with one object per article, with this exact shape:
[
  {
    "title": "<original title>",
    "url": "<original url>",
    "signal_score": 8,
    "reason": "<why this is important or not>",
    "summary": "<short summary in 1-3 sentences>"
  }
]

Do NOT include any additional keys. Do NOT include comments or markdown. Only output valid JSON.
    """.strip()

    return f"{instruction}\n\nArticles:\n{articles_block}"


def _call_gemini(
    prompt: str,
    timeout: float,
    model_name: Optional[str] = None,
) -> str:
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise LLMAnalysisError("GOOGLE_API_KEY is not set in the environment.")

    genai.configure(api_key=api_key)
    model_id = model_name or os.getenv("LLM_MODEL_GEMINI", "gemini-2.5-flash")
    model = genai.GenerativeModel(model_id)

    def _invoke() -> str:
        # Align SDK HTTP deadline with the outer executor timeout so the client does not fail first.
        response = model.generate_content(
            prompt,
            request_options={"timeout": timeout},
        )
        if not response or not getattr(response, "text", None):
            raise LLMAnalysisError("Gemini returned an empty response.")
        return response.text  # type: ignore[return-value]

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_invoke)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeoutError as exc:
            raise LLMAnalysisError(f"Gemini call timed out after {timeout} seconds.") from exc


def _call_openai(
    prompt: str,
    timeout: float,
    model_name: Optional[str] = None,
) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise LLMAnalysisError("OPENAI_API_KEY is not set in the environment.")

    client = OpenAI(api_key=api_key, timeout=timeout)
    model_id = model_name or os.getenv("LLM_MODEL_OPENAI", "gpt-4.1-mini")

    try:
        response = client.chat.completions.create(
            model=model_id,
            messages=[
                {
                    "role": "system",
                    "content": "You are a JSON-only API that returns strictly valid JSON.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.1,
        )
    except Exception as exc:  # pragma: no cover - network layer
        raise LLMAnalysisError(f"OpenAI API call failed: {exc}") from exc

    try:
        content = response.choices[0].message.content
    except (AttributeError, IndexError) as exc:
        raise LLMAnalysisError("OpenAI returned an unexpected response structure.") from exc

    if not content:
        raise LLMAnalysisError("OpenAI returned an empty response.")
    return content


def _parse_llm_output(raw: str) -> List[SignalItem]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LLMAnalysisError(f"Failed to parse LLM JSON output: {exc}\nRaw output:\n{raw}") from exc

    if not isinstance(data, list):
        raise LLMAnalysisError("LLM output is not a JSON array.")

    items: List[SignalItem] = []
    for obj in data:
        try:
            item = SignalItem.model_validate(obj)
        except ValidationError as exc:
            raise LLMAnalysisError(f"Invalid signal item structure: {exc}") from exc
        items.append(item)

    return items


def analyze_articles(
    articles: Sequence[Article],
    timeout: float = DEFAULT_ANALYSIS_TIMEOUT_SECONDS,
) -> List[SignalItem]:
    """
    Analyze a list of articles using an LLM and return structured signal items.

    Args:
        articles: Sequence of Article objects to evaluate.
        timeout: Maximum time in seconds to wait for the full LLM response (default 120).
            Large batches may require most of this budget; the call is only aborted after
            this limit is exceeded.

    Returns:
        List of SignalItem instances.

    Raises:
        LLMAnalysisError: If the LLM call fails or returns invalid data.
    """
    if not articles:
        return []

    prompt = _build_prompt(articles)
    provider = _get_llm_provider()

    if provider == "gemini":
        raw = _call_gemini(prompt=prompt, timeout=timeout)
    else:
        raw = _call_openai(prompt=prompt, timeout=timeout)

    return _parse_llm_output(raw)

