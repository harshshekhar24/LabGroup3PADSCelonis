# Streamlit-template
This Repo contains a template that can be used for the PADS software praktikum to create streamlit prototypes.

### Requirements
- [Python 3.11](https://www.python.org/downloads/) we recommend to use pyenv to maintain multiple python versions.
- [Poetry](https://www.python-poetry.org/)

### setup

1. make sure to install the above requirements correctly.
2. Install all needed dependencies by executing:
```console
poetry install --sync
```

### Execution
To run the prototype, you need to start it in the correct working directory. The standard way to start streamlit (`poetry run streamlit run prototype/<name_of_prototype>/main.py`) does not work in the setting of a package, as the working directory then starts at the streamlit main.

Thus for the prototype to work one needs to run from the root of the package:
```console
poetry run python -m streamlit run prototypes/<name_of_prototype>/main.py
```
Another way to run the prototype is via this command:
```commandline
poetry run python run prototypes/<name_of_prototype>/bootstrap.py
```
The bootstrap.py file can also be executed using the play button within IntelliJ.

### Running Pre-commit-hooks
To run the linters and formatters specified in your pre-commit hooks, simply execute:

```bash
poetry run pre-commit run --all-files
```

### Running Tests
To run all tests locally, simply execute:

```bash
poetry run pytest -n=auto tests
```
