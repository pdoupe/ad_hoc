# .PHONY: Declares targets that are not actual files, ensuring they always run.
.PHONY: test coverage coverage-html clean

# Define the path to your cohorts directory relative to the Makefile's location.
# This assumes the Makefile is in the parent directory of 'cohorts'.
COHORTS_DIR = cohorts

# Default target: runs all tests
all: test

# Target to run all pytest tests
test:
	@echo "Running all tests in the '$(COHORTS_DIR)' directory..."
	# Change into the cohorts directory to ensure poetry and pytest find
	# pyproject.toml and the test files correctly.
	@cd $(COHORTS_DIR) && poetry run pytest

# Target to run tests and generate a terminal coverage report
coverage:
	@echo "Running tests and generating terminal coverage report for '$(COHORTS_DIR)'..."
	@cd $(COHORTS_DIR) && poetry run pytest --cov=. --cov-report=term-missing

# Target to run tests and generate a detailed HTML coverage report
coverage-html:
	@echo "Running tests and generating HTML coverage report for '$(COHORTS_DIR)'..."
	@cd $(COHORTS_DIR) && poetry run pytest --cov=. --cov-report=html
	@echo "HTML coverage report generated in $(COHORTS_DIR)/htmlcov/. Open $(COHORTS_DIR)/htmlcov/index.html in your web browser."

# Target to clean up generated files (e.g., __pycache__, .pytest_cache, htmlcov)
clean:
	@echo "Cleaning up build artifacts and cache directories in '$(COHORTS_DIR)'..."
	# Remove __pycache__ directories
	@find $(COHORTS_DIR) -depth -name "__pycache__" -exec rm -rf {} +
	# Remove .pyc compiled files
	@find $(COHORTS_DIR) -depth -name "*.pyc" -exec rm -rf {} +
	# Remove pytest cache directory
	@rm -rf $(COHORTS_DIR)/.pytest_cache/
	# Remove coverage HTML report directory
	@rm -rf $(COHORTS_DIR)/htmlcov/
	@echo "Cleanup complete."

