# Contributing

Thanks for your interest in contributing! For small changes, open a PR. For larger changes, please open an issue to discuss first.

## Dev Setup
- Install Docker Desktop and Make
- `cp .env.example .env`
- `make up && make airflow-init`
- `make submit-stream` (keeps running) and `make producer`

## Style
- Python: ruff + black (see `.pre-commit-config.yaml`)
- Keep code readable and well-typed where possible.

## Tests
- `pytest -q`
- Integration tests use Testcontainers for Kafka/Postgres where applicable.

## CI
- GitHub Actions runs lint and tests on PRs.

## License
By contributing, you agree your contributions will be licensed under the MIT license.

