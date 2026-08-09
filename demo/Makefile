.PHONY: help setup demo ui list clean
.DEFAULT_GOAL := help

help:  ## Show these targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-8s\033[0m %s\n", $$1, $$2}'

setup:  ## Install dependencies and apply migrations
	uv sync
	uv run python manage.py migrate

demo:  ## Run the whole verification suite and print the PASS/FAIL matrix
	uv run python manage.py demo all

ui:  ## Boot the clusters and serve the monitoring dashboard on :8000
	uv run python manage.py demo serve

list:  ## List every scenario and what it proves
	uv run python manage.py demo list

clean:  ## Remove cluster logs
	rm -rf .demo-logs
