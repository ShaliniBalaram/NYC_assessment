# Contributing to NYC TLC Data Pipeline

Thank you for your interest in contributing to the NYC TLC Data Pipeline project! This document provides guidelines and instructions for contributing.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation](#documentation)

## Code of Conduct

This project adheres to a Code of Conduct that all contributors are expected to follow. Please read [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) before contributing.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/your-username/nyc-tlc-pipeline.git
   cd nyc-tlc-pipeline
   ```
3. Add the upstream repository:
   ```bash
   git remote add upstream https://github.com/original-owner/nyc-tlc-pipeline.git
   ```

## Development Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   .\venv\Scripts\activate  # Windows
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

3. Set up pre-commit hooks:
   ```bash
   pre-commit install
   ```

## Pull Request Process

1. Create a new branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and commit:
   ```bash
   git add .
   git commit -m "Description of changes"
   ```

3. Keep your branch updated:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

4. Push your changes:
   ```bash
   git push origin feature/your-feature-name
   ```

5. Create a Pull Request through GitHub

## Coding Standards

We follow PEP 8 with some modifications:

1. Line Length: 88 characters (Black formatter default)
2. Import Order:
   ```python
   # Standard library
   import os
   import sys
   
   # Third-party packages
   import numpy as np
   import pandas as pd
   
   # Local modules
   from src.ingestion import nifi_controller
   ```

3. Documentation:
   ```python
   def process_data(df: pd.DataFrame, column: str) -> pd.DataFrame:
       """
       Process the input DataFrame by applying transformations.
       
       Args:
           df (pd.DataFrame): Input DataFrame
           column (str): Column to process
           
       Returns:
           pd.DataFrame: Processed DataFrame
           
       Raises:
           ValueError: If column not in DataFrame
       """
       pass
   ```

## Testing Guidelines

1. Test File Structure:
   ```
   tests/
   ├── unit/
   │   ├── test_ingestion.py
   │   ├── test_processing.py
   │   └── test_visualization.py
   └── integration/
       ├── test_pipeline.py
       └── test_end_to_end.py
   ```

2. Test Naming:
   ```python
   def test_should_transform_data_when_valid_input():
       pass
   
   def test_should_raise_error_when_invalid_column():
       pass
   ```

3. Running Tests:
   ```bash
   # Run all tests
   pytest
   
   # Run specific test file
   pytest tests/unit/test_ingestion.py
   
   # Run with coverage
   pytest --cov=src tests/
   ```

## Documentation

1. Code Documentation:
   - All modules should have docstrings
   - All public functions should have docstrings
   - Complex logic should have inline comments

2. Project Documentation:
   - Update README.md for major changes
   - Add/update architecture diagrams
   - Document configuration changes

3. Example Documentation Format:
   ```python
   """
   Module for data transformation operations.
   
   This module provides functions for cleaning and transforming
   NYC TLC trip data using Apache Spark.
   
   Typical usage example:
   
   from src.processing import data_transformer
   
   transformer = data_transformer.DataTransformer()
   transformed_df = transformer.process_trip_data(raw_df)
   """
   ```

## Version Control

1. Branch Naming:
   - feature/: New features
   - bugfix/: Bug fixes
   - docs/: Documentation updates
   - test/: Test additions/modifications

2. Commit Messages:
   ```
   feat: Add new data validation step
   fix: Correct timestamp parsing in trip data
   docs: Update deployment instructions
   test: Add unit tests for data transformer
   ```

## Release Process

1. Version Numbering:
   - Follow semantic versioning (MAJOR.MINOR.PATCH)
   - Document changes in CHANGELOG.md

2. Release Steps:
   - Update version numbers
   - Update CHANGELOG.md
   - Create release branch
   - Create GitHub release with notes

## Getting Help

- Create an issue for bugs or feature requests
- Join our community discussions
- Contact maintainers for urgent issues

Remember to:
- Write clear commit messages
- Update tests for new features
- Document your changes
- Follow the code style guide
