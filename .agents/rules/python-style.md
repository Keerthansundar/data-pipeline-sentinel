# Python Style Guidelines

- **Python Version:** Python 3.12+ compatible.
- **Typing:** Strict type annotations on all function signatures, return types, and class attributes (`from typing import Optional, List, Dict, Any, Union, Tuple`).
- **Docstrings:** Google or Sphinx style docstrings on all public functions, classes, and methods detailing arguments, return values, and exceptions raised.
- **Formatting and Linting:** Clean according to `ruff check` and `ruff format`. Line length limit is 100 characters.
- **Path Handling:** Always use `pathlib.Path` for file system paths rather than string concatenation or `os.path`.
- **Modular Design:** Small files (< 300 lines preferred), single responsibility, explicit exports.
- **Configuration:** Load configuration via typed Pydantic models or clean dictionaries with default fallbacks from environment variables.
- **Exceptions:** Define domain-specific custom exceptions in `pipeline/errors.py` or module-specific errors rather than raising generic `Exception`.
