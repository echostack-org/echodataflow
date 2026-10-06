# Tests Layout

This repository keeps tests in a top-level `tests/` directory so test code is separated from importable package code.

## Structure

- `tests/flow/`: tests for `echodataflow.flows`
- `tests/operations/`: tests for `echodataflow.operations`
- `tests/utils/`: tests for `echodataflow.utils`
- `tests/deployment/`: tests for `echodataflow.deployment`
- `tests/tasks/`: tests for `echodataflow.tasks`
- `tests/services/`: reserved for tests for `echodataflow.services`

## Conventions

- Name files as `test_*.py`.
- Prefer behavior-focused tests that import package modules the same way users do.
- Keep fixtures and stubs local to the test module unless they are reused in multiple files.

## Running Tests

Run all configured tests:

```bash
python -m pytest
```

Run deployment tests only (substitute any folder above to select another group):

```bash
python -m pytest tests/deployment
```

If your environment does not include optional pytest plugins configured in `pyproject.toml`, you can temporarily override addopts:

```bash
python -m pytest -o addopts='' tests/deployment
```
