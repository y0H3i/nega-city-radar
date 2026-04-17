# Nega-City: Deep-Signal Radar

A decentralized intelligence node designed to invert the noise of late-stage capitalism into pure technical signals.

## Project Structure

This is a monorepo consisting of two primary components:

- **/core**: The autonomous backend engine. Built with Go, Python, Gemini 2.5 Flash, and PostgreSQL. It scrapes, filters, and archives high-purity technical signals.
- **/web**: The visualization console. A minimalist, dark-mode dashboard built with Next.js and Tailwind CSS.

## Getting Started

1. Set up the environment variables in both `/core` and `/web`.
2. Launch the infrastructure in `/core`: `docker-compose up -d`.
3. Start the backend in `/core`: `go run main.go`.
4. Start the frontend in `/web`: `npm run dev`.

Explore the radar at `http://localhost:3000`.