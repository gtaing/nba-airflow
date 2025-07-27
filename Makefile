PYTHON_PATH=.venv/bin/python

.PHONY: tests
tests:
	$(PYTHON_PATH) -m pytest -v tests/


.PHONY: clean-target
clean-target:
	@echo "Cleaning the /target folder..."
	@rm -rf target/*