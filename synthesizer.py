import sys
import json
import os
import google.generativeai as genai

# Configure the Gemini API client
genai.configure(api_key=os.environ.get("GOOGLE_API_KEY"))

def synthesize_report(signals_json: str) -> dict:
    """
    Synthesizes a high-level intelligence report from a JSON array of signals.
    """
    model = genai.GenerativeModel('gemini-2.5-flash')

    prompt = f"""You are an expert intelligence analyst specializing in technological trends and architectural paradigm shifts.
Your task is to synthesize a high-level intelligence report based on the provided raw signals.
Identify overarching meta-trends, emerging architectural patterns, and significant shifts in how technology is being developed, deployed, or used.
Focus on identifying insights that would be valuable for strategic decision-making.

The report should be structured as follows:
- A concise, impactful title.
- An introduction summarizing the overall findings.
- Sections dedicated to identified meta-trends, architectural paradigm shifts, and any other notable observations.
- Use Markdown formatting for readability.

Provided signals (JSON array):
{signals_json}

Your output MUST be a JSON object with two keys: "title" (string) and "content" (string, containing the Markdown report).
Example:
{{
  "title": "Emerging Trends in AI and Cloud Computing",
  "content": "# Emerging Trends in AI and Cloud Computing\\n\\nThis report synthesizes key signals from recent developments, highlighting significant meta-trends and architectural shifts.\\n\\n## Meta-Trends\\n\\n*   **Trend 1:** ...\\n*   **Trend 2:** ...\\n\\n## Architectural Paradigm Shifts\\n\\n*   **Shift 1:** ...\\n*   **Shift 2:** ...\\n\\n"
}}
"""
    try:
        response = model.generate_content(prompt)
        report_content = response.text

        # Attempt to parse the LLM's output as JSON
        report_obj = json.loads(report_content)
        if "title" not in report_obj or "content" not in report_obj:
            raise ValueError("LLM output missing 'title' or 'content' keys.")
        return report_obj
    except Exception as e:
        # Fallback in case LLM doesn't return perfect JSON or fails
        error_title = "Synthesis Report Generation Failed"
        error_content = f"Failed to generate report due to an internal error or malformed LLM output: {e}\n\nLLM Raw Output (if available):\n```\n{response.text if 'response' in locals() else 'N/A'}\n```"
        return {"title": error_title, "content": error_content}


if __name__ == "__main__":
    signals_input = sys.stdin.read()
    report = synthesize_report(signals_input)
    json.dump(report, sys.stdout)
    sys.stdout.write("\n") # Ensure a newline at the end
