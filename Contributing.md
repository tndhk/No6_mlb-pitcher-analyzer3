# CONTRIBUTING.md
# Contributing to MLB Pitcher Dashboard

We love your input! We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## Development Process

We use GitHub to host code, to track issues and feature requests, as well as accept pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Pull Requests

1. Update the README.md with details of changes if needed
2. Update the documentation if needed
3. The PR should work with the main test suite passing
4. Be sure to merge the latest from "upstream" before making a pull request

## Testing

We use pytest for testing. All tests should pass before submitting a PR:

```bash
pytest
```

For more detailed test output:

```bash
pytest -xvs
```

## Code Style

We use black, isort, and pylint for code style and formatting:

```bash
black src tests
isort src tests
pylint src tests
```

## License

By contributing, you agree that your contributions will be licensed under the project's license.