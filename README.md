# dcmon

`dcmon` is a Textual UI for inspecting running Docker Compose services and switching git-backed mounts across matching worktrees.

## Development

- `make lint`
- `make test`
- `make run`

## Release Assets

The GitHub Actions workflow publishes standalone archives for:

- `macOS arm64`
- `Linux x86_64`

For branch and pull request runs, download the build from the workflow artifacts. For tagged releases, download the same archives from the GitHub Releases page.

## Running A Downloaded Build

1. Download and unpack the archive for your platform.
2. Run `./dcmon --version` to verify the binary starts.
3. Run `./dcmon` from a terminal on a host with Docker and Docker Compose available.

`dcmon` expects to inspect local Docker containers. It does not bundle Docker itself.

## macOS Quarantine

GitHub-downloaded binaries may be blocked by Gatekeeper. If macOS refuses to open `dcmon`, remove the quarantine attribute:

```bash
xattr -d com.apple.quarantine ./dcmon
```

Then run the binary again from the terminal.
