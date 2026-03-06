## Overview

This library provides shared functionality and utilities used across EPS (Electronic Prescription Service) and Spine systems.

## Development

It is recommended that you use VS Code and a devcontainer as this will install all necessary components and correct versions of tools and languages.
See https://code.visualstudio.com/docs/devcontainers/containers for details on how to set this up on your host machine.

All commits must be made using [signed commits](https://docs.github.com/en/authentication/managing-commit-signature-verification/signing-commits)

Once the steps at the link above have been completed. Add to your ~/.gnupg/gpg.conf as below:

```
use-agent
pinentry-mode loopback
```

and to your ~/.gnupg/gpg-agent.conf as below:

```
allow-loopback-pinentry
```

As described here:
https://stackoverflow.com/a/59170001

You will need to create the files, if they do not already exist.
This will ensure that your VSCode bash terminal prompts you for your GPG key password.

You can cache the gpg key passphrase by following instructions at https://superuser.com/questions/624343/keep-gnupg-credentials-cached-for-entire-user-session

### Pre-commit Hooks

Some pre-commit hooks are installed as part of the install above, to run basic lint checks and ensure you can't accidentally commit invalid changes.
The pre-commit hook uses python package pre-commit and is configured in the file .pre-commit-config.yaml.
A combination of these checks are also run in CI.

### Code Quality

This project uses Flake8 for linting and Black for code formatting.

Run linting:

```bash
make lint
```

Format code:

```bash
make format
```
