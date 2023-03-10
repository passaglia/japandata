# Makefile
SHELL = /bin/bash

.PHONY: help
help:
    @echo "Commands:"
    @echo "venv    : creates a virtual environment."
    @echo "jupytervenv    : adds the venv kernel to jupyter."
    @echo "jupyter    : launches jupyter."
    @echo "style   : executes style formatting."
    @echo "build   : builds the package."
    @echo "pypi    : uploads the package to pypi."

# Styling
.PHONY: style
style:
	source venv/bin/activate && \
	black . && \
	isort . && \
	flake8

# Environment
venv:
	python3 -m venv venv
	source venv/bin/activate && \
	python3 -m pip install pip setuptools wheel && \
	python3 -m pip install -e .

# Jupyter Venv
jupytervenv:
	source venv/bin/activate && \
	pip install ipykernel && \
	pip install jupyter && \
	python -m ipykernel install --name venv --user

# Jupyter
jupyter:
	source venv/bin/activate && \
	jupyter lab

# Build
build:
	python3 -m build

# Pypi
pypi:
	pip install twine
	python3 -m twine upload dist/* --verbose
	# username: __token__ password: api key from pypi

