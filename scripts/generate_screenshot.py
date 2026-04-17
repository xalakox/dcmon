#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "textual>=0.83",
# ]
# ///
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from rich.text import Text
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Footer, Header, Static

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import dcmon


def make_service(
    *,
    project_name: str,
    service_name: str,
    repo_root: str,
    branch: str,
    is_worktree: bool = False,
    main_repo_path: str | None = None,
    extra_git_mount: bool = False,
) -> dcmon.ServiceInfo:
    git = dcmon.GitProbeInfo(
        repo_root=repo_root,
        repo_name=Path(repo_root).name,
        branch=branch,
        is_worktree=is_worktree,
        main_repo_path=main_repo_path,
        common_dir=f"{main_repo_path or repo_root}/.git",
        ticket_token=dcmon.extract_ticket_token(branch)
        or dcmon.extract_ticket_token(Path(repo_root).name),
    )

    mounts = [dcmon.MountInfo(kind="bind", source=repo_root, target="/app", git=git)]
    if extra_git_mount:
        mounts.append(
            dcmon.MountInfo(
                kind="bind",
                source="/workspace/sample-suite/shared-lib",
                target="/shared-lib",
                git=dcmon.GitProbeInfo(
                    repo_root="/workspace/sample-suite/shared-lib",
                    repo_name="shared-lib",
                    branch="main",
                    common_dir="/workspace/sample-suite/shared-lib/.git",
                ),
            )
        )
    mounts.append(
        dcmon.MountInfo(
            kind="volume",
            source="/var/lib/docker/volumes/sample-node-modules/_data",
            target="/app/node_modules",
            name="sample-node-modules",
            read_only=extra_git_mount,
        )
    )

    service = dcmon.ServiceInfo(
        project_name=project_name,
        service_name=service_name,
        compose_workdir="/workspace/sample-suite/sample-stack",
        compose_config_files=(
            "/workspace/sample-suite/sample-stack/docker-compose.yml",
        ),
        containers=[
            dcmon.ContainerInfo(
                container_id=f"{service_name}-id",
                name=service_name,
                service=service_name,
                state="running",
                mounts=mounts,
            )
        ],
    )
    dcmon.summarize_service(service)
    return service


SERVICES = [
    make_service(
        project_name="sample-stack",
        service_name="backend",
        repo_root="/workspace/sample-suite/api-service",
        branch="main",
    ),
    make_service(
        project_name="sample-stack",
        service_name="web-client",
        repo_root="/workspace/sample-suite/web-client_PROJ-123",
        branch="PROJ-123",
        is_worktree=True,
        main_repo_path="/workspace/sample-suite/web-client",
    ),
    make_service(
        project_name="sample-stack",
        service_name="worker",
        repo_root="/workspace/sample-suite/worker-service",
        branch="main",
        extra_git_mount=True,
    ),
]


class DemoScreenshotApp(App[None]):
    TITLE = "dcmon"
    BINDINGS = [
        ("q", "quit", "Quit"),
        ("j", "cursor_down_vim", "Down"),
        ("k", "cursor_up_vim", "Up"),
        ("g", "cursor_top_vim", "Top"),
        ("G", "cursor_bottom_vim", "Bottom"),
        ("l", "view_logs", "Logs"),
        ("s", "switch_ticket", "Switch"),
    ]
    CSS = """
    Screen {
      layout: vertical;
    }

    #services {
      height: 60%;
    }

    #details {
      height: 1fr;
      padding: 1 2;
      border-top: solid $surface;
      overflow-y: auto;
    }

    #status-banner {
      height: 1;
      padding: 0 2;
      content-align: left middle;
      text-style: bold;
      background: #1f4d5a;
      color: #d7f5ff;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header()
        yield DataTable(id="services")
        yield Static("", id="details")
        yield Static("demo snapshot using sanitized sample data", id="status-banner")
        yield Footer()

    def on_mount(self) -> None:
        self.sub_title = "demo snapshot"
        table = self.query_one("#services", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_columns("Project", "Service", "Repo", "Branch", "WT", "Extra")

        ordered_services = sorted(SERVICES, key=dcmon.service_sort_key)
        selected_row = 0

        for index, service in enumerate(ordered_services):
            row_style = dcmon.row_style_for_service(service)

            def cell(value: str) -> object:
                return Text(value, style=row_style) if row_style else value

            table.add_row(
                cell(service.project_name if index == 0 else ""),
                cell(service.service_name),
                cell(service.primary_repo_name),
                cell(service.primary_branch),
                cell(dcmon.format_worktree_flag(service.primary_is_worktree)),
                cell(
                    f"+{service.extra_git_mounts}" if service.extra_git_mounts else ""
                ),
            )
            if service.service_name == "web-client":
                selected_row = index

        table.move_cursor(row=selected_row, animate=False)
        selected_service = next(
            service
            for service in ordered_services
            if service.service_name == "web-client"
        )
        self.query_one("#details", Static).update(
            dcmon.render_detail_text(selected_service, None)
        )


async def main() -> None:
    output_path = (
        Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "docs" / "screenshot.svg"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    app = DemoScreenshotApp()
    async with app.run_test(size=(120, 38)) as pilot:
        await pilot.pause()
        app.save_screenshot(filename=output_path.name, path=str(output_path.parent))

    print(output_path)


if __name__ == "__main__":
    asyncio.run(main())
