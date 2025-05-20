# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Thoughtfull is an in-progress tool for aggregating Slack threads, personal notes, and other reference material in a single searchable database. The goal is to make it easy to collect snippets of information from multiple sources and retrieve them quickly.

## Repository Structure

The project is in early stages with minimal structure:
- Python-based project using Python 3.11+
- Uses `uv` for dependency management
- Core dependencies include: pydantic, loguru, tenacity
- Test dependencies: pytest

## Development Commands

### Environment Setup
```bash
# Clone the repository
git clone https://github.com/username/thoughtfull.git
cd thoughtfull

# Install dependencies using uv
uv venv
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows

# Install dependencies
uv pip install -e .
# Install with test dependencies
uv pip install -e ".[test]"
```

### Testing
```bash
# Run all tests
pytest

# Run a specific test file
pytest path/to/test_file.py

# Run a specific test function
pytest path/to/test_file.py::test_function_name
```

## Development Guidelines

As the project matures, additional sections will be added here to provide more specific guidance for development.