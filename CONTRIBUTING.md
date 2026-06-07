# Contributing to LigoTopology

Thank you for your interest in contributing to LigoTopology! We welcome contributions from the community to help improve this middleware.

This guide outlines the development workflow and coding standards for our project.

## 🛠️ Setting Up Your Local Environment

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/tyt6756/ligotopology.git
   cd ligotopology
   ```

2. **Create a Virtual Environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install Dependencies in Editable Mode**:
   Install LigoTopology along with its development dependencies (`pytest`, `pytest-asyncio`):
   ```bash
   pip install -e .[dev]
   ```

## 🧪 Testing

We use `pytest` as our primary testing framework. Please make sure all tests pass locally before proposing changes:

- Run the full test suite:
  ```bash
  pytest
  ```
- Run tests in quiet mode:
  ```bash
  pytest -q
  ```

## 📝 Coding Standards

- **Formatting**: We follow standard PEP 8 formatting guidelines.
- **Asynchronous Safety**: When modifying `MatrixRouter`, ensure lock safety. Always keep lock acquisition paths as small and non-blocking as possible.
- **Typing**: Use standard Python type hinting (`typing` module) for public APIs.

## 🚀 Proposing a Pull Request

1. Fork the repository and create your branch from `main`.
2. Write unit tests for new features or bug fixes.
3. Commit your changes using descriptive commit messages.
4. Push your branch to GitHub and open a Pull Request.
5. Ensure that the GitHub Actions CI workflow passes on your PR.
