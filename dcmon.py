#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "textual>=0.83",
# ]
# ///
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional, Sequence, Tuple, Union


REFRESH_SECONDS = 3.0
DEFAULT_TIMEOUT = 5.0
SWITCH_TIMEOUT = 45.0
STATUS_BANNER_TIMEOUT = 5.0
LOG_REFRESH_SECONDS = 1.0
LOG_TIMEOUT = 10.0
LOG_TAIL_LINES = 200
WORKTREE_OVERRIDE_PREFIX = "docker-compose.worktree.override"
WORKTREE_OVERRIDE_DIR = Path("/tmp")
BASE_BRANCH_NAMES = {
    "develop",
    "development",
    "main",
    "master",
    "prod",
    "production",
    "release",
    "staging",
}
BASE_ROW_STYLE = "#8ea9c0"
WORKTREE_ROW_STYLE = "#8faf90"
TICKET_PATTERN = re.compile(r"([A-Z][A-Z0-9]+-\d+)", re.IGNORECASE)
APP_VERSION_ENV = "DCMON_VERSION"
APP_VERSION_RESOURCE = "dcmon.version"
DEFAULT_APP_VERSION = "dev"


class DcmonProbeError(RuntimeError):
    pass


@dataclass
class GitProbeInfo:
    repo_root: Optional[str] = None
    repo_name: Optional[str] = None
    branch: Optional[str] = None
    is_worktree: bool = False
    main_repo_path: Optional[str] = None
    common_dir: Optional[str] = None
    ticket_token: Optional[str] = None


@dataclass
class MountInfo:
    kind: str
    source: Optional[str]
    target: str
    name: Optional[str] = None
    read_only: bool = False
    git: Optional[GitProbeInfo] = None


@dataclass
class ContainerInfo:
    container_id: str
    name: str
    service: str
    state: str
    mounts: list[MountInfo] = field(default_factory=list)


@dataclass
class ServiceInfo:
    project_name: str
    service_name: str
    compose_workdir: Optional[str]
    compose_config_files: tuple[str, ...] = ()
    containers: list[ContainerInfo] = field(default_factory=list)
    primary_repo_name: str = "-"
    primary_branch: str = "-"
    primary_is_worktree: bool = False
    extra_git_mounts: int = 0
    error: Optional[str] = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.project_name, self.service_name)


@dataclass
class WorkspaceCheckout:
    checkout_path: str
    repo_name: str
    branch: str
    common_dir: str
    is_worktree: bool
    token: Optional[str]
    branch_token: Optional[str]


@dataclass
class RepoFamily:
    common_dir: str
    primary_checkout_path: str
    checkouts: list[WorkspaceCheckout] = field(default_factory=list)


@dataclass
class WorkspaceIndex:
    scan_roots: tuple[str, ...] = ()
    families_by_common_dir: dict[str, RepoFamily] = field(default_factory=dict)
    tokens: dict[str, list[WorkspaceCheckout]] = field(default_factory=dict)


@dataclass
class MountSwitchPlan:
    mount: MountInfo
    current_source: Optional[str]
    planned_source: Optional[str]
    resolution: str
    note: str
    changed: bool = False


@dataclass
class ServiceSwitchPlan:
    ticket_token: str
    service: ServiceInfo
    durable_compose_files: tuple[str, ...]
    mount_plans: list[MountSwitchPlan] = field(default_factory=list)
    skipped_reason: Optional[str] = None
    apply_error: Optional[str] = None

    @property
    def project_name(self) -> str:
        return self.service.project_name

    @property
    def service_name(self) -> str:
        return self.service.service_name

    @property
    def compose_workdir(self) -> Optional[str]:
        return self.service.compose_workdir

    @property
    def service_label(self) -> str:
        return f"{self.project_name}/{self.service_name}"

    def changed_git_mounts(self) -> list[MountSwitchPlan]:
        return [
            mount_plan
            for mount_plan in self.mount_plans
            if mount_plan.mount.kind == "bind" and mount_plan.changed and mount_plan.resolution in {"ticket", "base"}
        ]

    def can_apply(self) -> bool:
        return self.skipped_reason is None and bool(self.changed_git_mounts())


@dataclass
class SwitchPlan:
    ticket_token: str
    service_plans: list[ServiceSwitchPlan] = field(default_factory=list)

    def changed_mount_count(self) -> int:
        return sum(len(service_plan.changed_git_mounts()) for service_plan in self.service_plans if service_plan.can_apply())

    def fallback_mount_count(self) -> int:
        total = 0
        for service_plan in self.service_plans:
            if not service_plan.can_apply():
                continue
            total += sum(1 for mount_plan in service_plan.changed_git_mounts() if mount_plan.resolution == "base")
        return total

    def skipped_service_count(self) -> int:
        return sum(1 for service_plan in self.service_plans if service_plan.skipped_reason)

    def executable_service_plans(self) -> list[ServiceSwitchPlan]:
        return [service_plan for service_plan in self.service_plans if service_plan.can_apply()]


@dataclass
class SwitchResult:
    applied_services: list[str] = field(default_factory=list)
    skipped_services: list[str] = field(default_factory=list)
    apply_errors: list[str] = field(default_factory=list)
    verification_errors: list[str] = field(default_factory=list)
    refreshed_services: Optional[list[ServiceInfo]] = None
    refresh_error: Optional[str] = None

    def summary(self, ticket_token: str) -> str:
        parts = [f"{ticket_token}"]
        if self.applied_services:
            parts.append(f"applied {len(self.applied_services)}")
        if self.skipped_services:
            parts.append(f"skipped {len(self.skipped_services)}")
        if self.apply_errors or self.verification_errors or self.refresh_error:
            parts.append("with warnings")
        return " | ".join(parts)


@dataclass
class TicketOption:
    token: str
    match_count: int
    repo_names: tuple[str, ...] = ()


def family_display_name(family: RepoFamily) -> str:
    primary_path = Path(family.primary_checkout_path).expanduser()
    return primary_path.name or family.primary_checkout_path


def version_resource_candidates() -> tuple[Path, ...]:
    candidates: list[Path] = []
    bundle_root = getattr(sys, "_MEIPASS", None)
    if bundle_root:
        candidates.append(Path(bundle_root) / APP_VERSION_RESOURCE)
    candidates.append(Path(__file__).resolve().with_name(APP_VERSION_RESOURCE))
    return tuple(candidates)


def resolve_app_version() -> str:
    env_version = os.getenv(APP_VERSION_ENV, "").strip()
    if env_version:
        return env_version
    for candidate in version_resource_candidates():
        try:
            version = candidate.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if version:
            return version
    return DEFAULT_APP_VERSION


def _run(cmd: Sequence[str], timeout: float = DEFAULT_TIMEOUT, cwd: Optional[str] = None) -> str:
    try:
        completed = subprocess.run(
            list(cmd),
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
    except FileNotFoundError as exc:
        raise DcmonProbeError(f"{cmd[0]} not found") from exc
    except subprocess.TimeoutExpired as exc:
        raise DcmonProbeError(f"{cmd[0]} timed out after {timeout:.1f}s") from exc

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        detail = stderr or stdout or f"exit code {completed.returncode}"
        raise DcmonProbeError(f"{cmd[0]} failed: {detail}")

    return completed.stdout


def _run_combined(cmd: Sequence[str], timeout: float = DEFAULT_TIMEOUT, cwd: Optional[str] = None) -> str:
    try:
        completed = subprocess.run(
            list(cmd),
            capture_output=True,
            check=False,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
    except FileNotFoundError as exc:
        raise DcmonProbeError(f"{cmd[0]} not found") from exc
    except subprocess.TimeoutExpired as exc:
        raise DcmonProbeError(f"{cmd[0]} timed out after {timeout:.1f}s") from exc

    if completed.returncode != 0:
        stderr = (completed.stderr or "").strip()
        stdout = (completed.stdout or "").strip()
        detail = stderr or stdout or f"exit code {completed.returncode}"
        raise DcmonProbeError(f"{cmd[0]} failed: {detail}")

    return f"{completed.stdout}{completed.stderr}"


def dispatch_app_callback(app: object, callback: Callable[..., None], *args: object) -> None:
    dispatcher = getattr(app, "call_from_thread", None)
    if not callable(dispatcher):
        raise DcmonProbeError("UI dispatcher unavailable")
    dispatcher(callback, *args)


def list_compose_container_ids() -> list[str]:
    output = _run(
        [
            "docker",
            "ps",
            "--filter",
            "label=com.docker.compose.project",
            "--format",
            "{{.ID}}",
            "--no-trunc",
        ]
    )
    return [line.strip() for line in output.splitlines() if line.strip()]


def inspect_containers(container_ids: Sequence[str]) -> list[dict]:
    if not container_ids:
        return []
    output = _run(["docker", "inspect", *container_ids])
    data = json.loads(output or "[]")
    if not isinstance(data, list):
        raise DcmonProbeError("docker inspect returned an unexpected payload")
    return data


def parse_mounts(inspect_entry: dict) -> list[MountInfo]:
    mounts: list[MountInfo] = []
    for mount in inspect_entry.get("Mounts") or []:
        kind = str(mount.get("Type") or "unknown")
        target = str(mount.get("Destination") or "")
        if not target:
            continue
        source = mount.get("Source")
        name = mount.get("Name")
        raw_rw = mount.get("RW")
        read_only = False
        if isinstance(raw_rw, bool):
            read_only = not raw_rw
        mode = str(mount.get("Mode") or "")
        if "ro" in [part.strip() for part in mode.split(",") if part.strip()]:
            read_only = True
        mounts.append(
            MountInfo(
                kind=kind,
                source=str(source) if source else None,
                target=target,
                name=str(name) if name else None,
                read_only=read_only,
            )
        )
    return mounts


def split_compose_config_files(config_files: Optional[str], working_dir: Optional[str] = None) -> tuple[str, ...]:
    if not config_files:
        return ()

    resolved: list[str] = []
    for raw_value in config_files.split(","):
        candidate = raw_value.strip()
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if not path.is_absolute() and working_dir:
            path = Path(working_dir).expanduser() / path
        resolved.append(str(path))
    return tuple(dict.fromkeys(resolved))


def fallback_workdir_from_config_files(config_files: Optional[str]) -> Optional[str]:
    raw_files = split_compose_config_files(config_files)
    if not raw_files:
        return None
    return str(Path(raw_files[0]).expanduser().parent)


def derive_compose_workdir(inspect_entries: Sequence[dict]) -> Optional[str]:
    for entry in inspect_entries:
        labels = (entry.get("Config") or {}).get("Labels") or {}
        workdir = labels.get("com.docker.compose.project.working_dir")
        if workdir:
            return str(workdir)

    for entry in inspect_entries:
        labels = (entry.get("Config") or {}).get("Labels") or {}
        derived = fallback_workdir_from_config_files(labels.get("com.docker.compose.project.config_files"))
        if derived:
            return derived
    return None


def derive_compose_config_files(inspect_entries: Sequence[dict], compose_workdir: Optional[str]) -> tuple[str, ...]:
    collected: list[str] = []
    for entry in inspect_entries:
        labels = (entry.get("Config") or {}).get("Labels") or {}
        working_dir = str(labels.get("com.docker.compose.project.working_dir") or compose_workdir or "")
        collected.extend(split_compose_config_files(labels.get("com.docker.compose.project.config_files"), working_dir))
    return tuple(dict.fromkeys(collected))


def is_temporary_worktree_override(path_value: str) -> bool:
    path = Path(path_value).expanduser()
    return path.parent == WORKTREE_OVERRIDE_DIR and path.name.startswith(WORKTREE_OVERRIDE_PREFIX)


def durable_compose_config_files(config_files: Sequence[str]) -> tuple[str, ...]:
    durable: list[str] = []
    for raw_path in config_files:
        path = Path(raw_path).expanduser()
        if is_temporary_worktree_override(str(path)):
            continue
        if path.exists():
            durable.append(str(path))
    return tuple(dict.fromkeys(durable))


def git_probe_target(source: str) -> Path:
    path = Path(source).expanduser()
    if path.is_dir():
        return path
    return path.parent


def git_rev_parse(path: str, *args: str) -> str:
    output = _run(["git", "-C", path, "rev-parse", *args])
    return output.strip()


def extract_ticket_token(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    match = TICKET_PATTERN.search(text)
    if not match:
        return None
    return match.group(1).upper()


def normalize_checkout_name(name: str) -> str:
    without_ticket = TICKET_PATTERN.sub("", name)
    collapsed = re.sub(r"[-_]+", "-", without_ticket)
    return collapsed.strip("-_").lower()


def probe_git_path(source: str) -> Optional[GitProbeInfo]:
    probe_path = git_probe_target(source)
    probe_path_str = str(probe_path)

    try:
        repo_root = git_rev_parse(probe_path_str, "--show-toplevel")
        branch = git_rev_parse(probe_path_str, "--abbrev-ref", "HEAD")
        if branch == "HEAD":
            short_sha = git_rev_parse(probe_path_str, "--short", "HEAD")
            branch = f"(detached {short_sha})"
        git_dir = git_rev_parse(probe_path_str, "--path-format=absolute", "--git-dir")
        common_dir = git_rev_parse(probe_path_str, "--path-format=absolute", "--git-common-dir")
    except DcmonProbeError:
        return None

    repo_root_path = Path(repo_root)
    is_worktree = Path(git_dir) != Path(common_dir)
    main_repo_path = str(Path(common_dir).parent) if is_worktree else None
    ticket_token = extract_ticket_token(branch) or extract_ticket_token(repo_root_path.name)
    return GitProbeInfo(
        repo_root=repo_root,
        repo_name=repo_root_path.name or repo_root,
        branch=branch,
        is_worktree=is_worktree,
        main_repo_path=main_repo_path,
        common_dir=common_dir,
        ticket_token=ticket_token,
    )


def mount_sort_key(mount: MountInfo) -> Tuple[str, str]:
    return (mount.target, mount.source or "")


def unique_service_mounts(service: ServiceInfo) -> list[MountInfo]:
    mounts: list[MountInfo] = []
    seen: set[tuple[str, str, str, str, bool]] = set()
    for container in service.containers:
        for mount in container.mounts:
            key = (mount.kind, mount.source or "", mount.target, mount.name or "", mount.read_only)
            if key in seen:
                continue
            seen.add(key)
            mounts.append(mount)
    return sorted(mounts, key=mount_sort_key)


def summarize_service(service: ServiceInfo) -> None:
    git_mounts: list[MountInfo] = []
    seen_git_mounts: set[tuple[str, str]] = set()
    for mount in unique_service_mounts(service):
        if mount.kind != "bind" or mount.git is None:
            continue
        key = (mount.source or "", mount.target)
        if key in seen_git_mounts:
            continue
        seen_git_mounts.add(key)
        git_mounts.append(mount)

    if not git_mounts:
        return

    primary_mount = next((mount for mount in git_mounts if mount.target == "/app"), git_mounts[0])
    primary_git = primary_mount.git
    if primary_git is not None:
        service.primary_repo_name = primary_git.repo_name or "-"
        service.primary_branch = primary_git.branch or "-"
        service.primary_is_worktree = primary_git.is_worktree
    service.extra_git_mounts = max(0, len(git_mounts) - 1)


def build_service_snapshot(inspect_entries: Sequence[dict]) -> Tuple[list[ServiceInfo], Optional[str]]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    errors: list[str] = []

    for entry in inspect_entries:
        labels = (entry.get("Config") or {}).get("Labels") or {}
        project = str(labels.get("com.docker.compose.project") or "").strip()
        service = str(labels.get("com.docker.compose.service") or "").strip()
        if not project or not service:
            continue
        grouped.setdefault((project, service), []).append(entry)

    git_cache: dict[str, Optional[GitProbeInfo]] = {}
    services: list[ServiceInfo] = []

    for (project, service_name), entries in sorted(grouped.items()):
        compose_workdir = derive_compose_workdir(entries)
        service_info = ServiceInfo(
            project_name=project,
            service_name=service_name,
            compose_workdir=compose_workdir,
            compose_config_files=derive_compose_config_files(entries, compose_workdir),
        )

        for entry in entries:
            mounts = parse_mounts(entry)
            for mount in mounts:
                if mount.kind != "bind" or not mount.source:
                    continue
                if mount.source not in git_cache:
                    git_cache[mount.source] = probe_git_path(mount.source)
                mount.git = git_cache[mount.source]

            container = ContainerInfo(
                container_id=str(entry.get("Id") or ""),
                name=str(entry.get("Name") or "").lstrip("/"),
                service=service_name,
                state=str(((entry.get("State") or {}).get("Status") or "unknown")),
                mounts=mounts,
            )
            service_info.containers.append(container)

        try:
            summarize_service(service_info)
        except Exception as exc:  # pragma: no cover - defensive path
            message = f"{project}/{service_name}: {exc}"
            service_info.error = message
            errors.append(message)

        services.append(service_info)

    error_text = "; ".join(errors) if errors else None
    return services, error_text


def gather_services() -> Tuple[Optional[list[ServiceInfo]], Optional[str]]:
    try:
        container_ids = list_compose_container_ids()
        inspect_entries = inspect_containers(container_ids)
    except DcmonProbeError as exc:
        return None, str(exc)

    try:
        services, build_error = build_service_snapshot(inspect_entries)
    except DcmonProbeError as exc:
        return None, str(exc)

    return services, build_error


def format_path(path_value: Optional[str]) -> str:
    if not path_value:
        return "-"

    path = Path(path_value).expanduser()
    home = Path.home()
    try:
        relative = path.relative_to(home)
    except ValueError:
        return str(path)
    return f"~/{relative}"


def format_worktree_flag(is_worktree: bool) -> str:
    return "W" if is_worktree else "-"


def is_base_branch(branch: str) -> bool:
    return branch.strip().lower() in BASE_BRANCH_NAMES


def row_style_for_service(service: ServiceInfo) -> Optional[str]:
    if service.primary_is_worktree:
        return WORKTREE_ROW_STYLE
    if service.primary_branch != "-" and is_base_branch(service.primary_branch):
        return BASE_ROW_STYLE
    return None


def service_sort_bucket(service: ServiceInfo) -> int:
    if service.primary_is_worktree:
        return 0
    if service.primary_branch != "-" and is_base_branch(service.primary_branch):
        return 1
    if service.primary_repo_name != "-" or service.primary_branch != "-":
        return 2
    return 3


def service_sort_key(service: ServiceInfo) -> Tuple[int, str, str, str]:
    return (
        service_sort_bucket(service),
        service.project_name,
        service.service_name,
        service.primary_repo_name,
    )


def mount_label(mount: MountInfo) -> str:
    if mount.kind == "bind":
        source = format_path(mount.source)
        text = f"bind  {source} -> {mount.target}"
        if mount.read_only:
            text += " [ro]"
        if mount.git and mount.git.repo_name and mount.git.branch:
            marker = " W" if mount.git.is_worktree else ""
            text += f"  [{mount.git.repo_name}@{mount.git.branch}{marker}]"
        return text
    if mount.kind == "volume":
        volume_name = mount.name or mount.source or "-"
        suffix = " [ro]" if mount.read_only else ""
        return f"vol   {volume_name} -> {mount.target}{suffix}"
    if mount.kind == "tmpfs":
        return f"tmpfs - -> {mount.target}"
    source = mount.source or mount.name or "-"
    suffix = " [ro]" if mount.read_only else ""
    return f"{mount.kind:<5} {source} -> {mount.target}{suffix}"


def detail_lines_for_service(service: ServiceInfo, last_error: Optional[str] = None) -> list[str]:
    lines: list[str] = []
    if last_error:
        lines.append(f"error: {last_error}")
        lines.append("")

    title = f"{service.project_name}/{service.service_name}"
    if service.primary_repo_name != "-" and service.primary_branch != "-":
        title += f"  {service.primary_repo_name}@{service.primary_branch}"
        if service.primary_is_worktree:
            title += "  [worktree]"
    lines.append(title)

    lines.append(f"compose dir: {format_path(service.compose_workdir)}")
    if service.primary_is_worktree:
        for mount in unique_service_mounts(service):
            if mount.git and mount.git.repo_name == service.primary_repo_name and mount.git.main_repo_path:
                lines.append(f"main checkout: {format_path(mount.git.main_repo_path)}")
                break

    lines.append("")
    if not service.containers:
        lines.append("(no containers)")
        return lines

    lines.append("containers:")
    for container in sorted(service.containers, key=lambda item: item.name):
        lines.append(f"  {container.name} [{container.state}]")
        if container.mounts:
            for mount in sorted(container.mounts, key=mount_sort_key):
                lines.append(f"    {mount_label(mount)}")
        else:
            lines.append("    (no mounts)")
    if service.error:
        lines.append("")
        lines.append(f"service error: {service.error}")
    return lines


def render_detail_text(service: Optional[ServiceInfo], last_error: Optional[str] = None) -> str:
    if service is None:
        lines = ["No running compose services."]
        if last_error:
            lines.extend(["", f"error: {last_error}"])
        return "\n".join(lines)
    return "\n".join(detail_lines_for_service(service, last_error))


def row_key_value(raw_key: object) -> str:
    value = getattr(raw_key, "value", raw_key)
    return str(value)


def iter_checkout_roots(scan_root: Path) -> list[Path]:
    roots: list[Path] = []
    if not scan_root.exists():
        return roots

    for current, dirnames, filenames in os.walk(scan_root):
        if ".git" in dirnames or ".git" in filenames:
            roots.append(Path(current))
            dirnames[:] = []
            continue
        dirnames[:] = [name for name in dirnames if name not in {".git", "__pycache__"}]
    return roots


def derive_workspace_scan_roots(services: Sequence[ServiceInfo]) -> tuple[Path, ...]:
    roots: set[Path] = set()
    for service in services:
        for mount in unique_service_mounts(service):
            if mount.kind != "bind" or mount.git is None:
                continue
            anchor = mount.git.main_repo_path or mount.git.repo_root
            if not anchor:
                continue
            anchor_path = Path(anchor).expanduser()
            roots.add(anchor_path.parent)
    return tuple(sorted(roots))


def workspace_checkout_from_probe(probe: GitProbeInfo) -> Optional[WorkspaceCheckout]:
    if not probe.repo_root or not probe.repo_name or not probe.branch or not probe.common_dir:
        return None
    return WorkspaceCheckout(
        checkout_path=probe.repo_root,
        repo_name=probe.repo_name,
        branch=probe.branch,
        common_dir=probe.common_dir,
        is_worktree=probe.is_worktree,
        token=extract_ticket_token(probe.branch) or extract_ticket_token(probe.repo_name),
        branch_token=extract_ticket_token(probe.branch),
    )


def choose_primary_checkout_path(checkouts: Sequence[WorkspaceCheckout], common_dir: str) -> str:
    for checkout in sorted(checkouts, key=lambda item: item.checkout_path):
        if not checkout.is_worktree:
            return checkout.checkout_path

    common_parent = str(Path(common_dir).parent)
    if Path(common_parent).exists():
        return common_parent
    return sorted(checkout.checkout_path for checkout in checkouts)[0]


def build_workspace_index(
    checkouts: Sequence[WorkspaceCheckout],
    scan_roots: Optional[Union[Sequence[Path], Path]] = None,
) -> WorkspaceIndex:
    families_by_common_dir: dict[str, RepoFamily] = {}
    for checkout in sorted(checkouts, key=lambda item: item.checkout_path):
        family = families_by_common_dir.get(checkout.common_dir)
        if family is None:
            family = RepoFamily(
                common_dir=checkout.common_dir,
                primary_checkout_path=checkout.checkout_path,
            )
            families_by_common_dir[checkout.common_dir] = family
        family.checkouts.append(checkout)

    tokens: dict[str, list[WorkspaceCheckout]] = {}
    for common_dir, family in families_by_common_dir.items():
        family.primary_checkout_path = choose_primary_checkout_path(family.checkouts, common_dir)
        for checkout in family.checkouts:
            if not checkout.token:
                continue
            tokens.setdefault(checkout.token, []).append(checkout)

    if isinstance(scan_roots, Path):
        normalized_roots = (scan_roots.expanduser(),)
    elif scan_roots:
        normalized_roots = tuple(Path(root).expanduser() for root in scan_roots)
    else:
        normalized_roots = ()
    return WorkspaceIndex(
        scan_roots=tuple(str(root) for root in normalized_roots),
        families_by_common_dir=families_by_common_dir,
        tokens=tokens,
    )


def scan_workspace_index(
    services: Optional[Sequence[ServiceInfo]] = None,
    scan_roots: Optional[Union[Sequence[Path], Path]] = None,
) -> WorkspaceIndex:
    if scan_roots is None:
        derived_roots = derive_workspace_scan_roots(services or [])
    elif isinstance(scan_roots, Path):
        derived_roots = (scan_roots.expanduser(),)
    else:
        derived_roots = tuple(Path(root).expanduser() for root in scan_roots)

    checkouts: list[WorkspaceCheckout] = []
    seen_roots: set[Path] = set()
    for scan_root in derived_roots:
        if scan_root in seen_roots:
            continue
        seen_roots.add(scan_root)
        for checkout_root in iter_checkout_roots(scan_root):
            probe = probe_git_path(str(checkout_root))
            if probe is None:
                continue
            checkout = workspace_checkout_from_probe(probe)
            if checkout is not None:
                checkouts.append(checkout)
    return build_workspace_index(checkouts, derived_roots)


def choose_checkout_for_ticket(family: RepoFamily, ticket_token: str) -> tuple[str, str]:
    matches = [
        checkout
        for checkout in family.checkouts
        if checkout.token == ticket_token
    ]
    if matches:
        chosen = sorted(
            matches,
            key=lambda checkout: (
                0 if checkout.branch_token == ticket_token else 1,
                checkout.checkout_path,
            ),
        )[0]
        return chosen.checkout_path, "ticket"
    return family.primary_checkout_path, "base"


def rebase_mount_source(source: str, repo_root: str, new_repo_root: str) -> str:
    source_path = Path(source).expanduser()
    repo_root_path = Path(repo_root).expanduser()
    new_root_path = Path(new_repo_root).expanduser()
    try:
        relative_path = source_path.relative_to(repo_root_path)
    except ValueError:
        return str(new_root_path)
    if str(relative_path) == ".":
        return str(new_root_path)
    return str(new_root_path / relative_path)


def family_checkout_aliases(family: RepoFamily) -> set[str]:
    aliases = {normalize_checkout_name(Path(family.primary_checkout_path).name)}
    for checkout in family.checkouts:
        aliases.add(normalize_checkout_name(checkout.repo_name))
        aliases.add(normalize_checkout_name(Path(checkout.checkout_path).name))
    return {alias for alias in aliases if alias}


def infer_repo_root_for_family(source: str, family: RepoFamily) -> Optional[str]:
    aliases = family_checkout_aliases(family)
    if not aliases:
        return None
    source_path = Path(source).expanduser()
    for candidate in (source_path, *source_path.parents):
        if normalize_checkout_name(candidate.name) in aliases:
            return str(candidate)
    return None


def resolve_mount_family(
    mount: MountInfo,
    workspace: WorkspaceIndex,
) -> tuple[Optional[RepoFamily], Optional[str]]:
    if mount.git is not None and mount.git.common_dir and mount.git.repo_root:
        family = workspace.families_by_common_dir.get(mount.git.common_dir)
        if family is not None:
            return family, mount.git.repo_root

    if not mount.source:
        return None, None

    matches: list[tuple[RepoFamily, str]] = []
    for family in workspace.families_by_common_dir.values():
        repo_root = infer_repo_root_for_family(mount.source, family)
        if repo_root is not None:
            matches.append((family, repo_root))

    if len(matches) != 1:
        return None, None
    return matches[0]


def derive_volume_name(source: Optional[str]) -> Optional[str]:
    if not source:
        return None
    path = Path(source)
    parts = path.parts
    if len(parts) >= 3 and parts[-1] == "_data" and parts[-3] == "volumes":
        return parts[-2]
    return None


def compose_mount_spec(mount: MountInfo, source_override: Optional[str] = None) -> Optional[str]:
    if mount.kind == "bind":
        source = source_override or mount.source
        if not source:
            return None
        suffix = ":ro" if mount.read_only else ""
        return f"{source}:{mount.target}{suffix}"
    if mount.kind == "volume":
        source = mount.name or derive_volume_name(mount.source)
        if not source:
            return None
        suffix = ":ro" if mount.read_only else ""
        return f"{source}:{mount.target}{suffix}"
    return None


def compose_config_mount_spec(entry: dict) -> Optional[str]:
    kind = str(entry.get("type") or "")
    target = str(entry.get("target") or "")
    if not target:
        return None
    read_only = bool(entry.get("read_only"))
    suffix = ":ro" if read_only else ""
    if kind == "bind":
        source = entry.get("source")
        if not source:
            return None
        return f"{source}:{target}{suffix}"
    if kind == "volume":
        source = entry.get("source")
        if source:
            return f"{source}:{target}{suffix}"
        return f"{target}{suffix}"
    return None


def load_compose_service_mount_specs(
    project_name: str,
    compose_workdir: Optional[str],
    config_files: Sequence[str],
) -> dict[str, dict[str, str]]:
    cmd = ["docker", "compose", "--project-name", project_name]
    for config_file in config_files:
        cmd.extend(["-f", config_file])
    cmd.extend(["config", "--format", "json"])
    output = _run(cmd, timeout=SWITCH_TIMEOUT, cwd=compose_workdir or None)
    try:
        payload = json.loads(output or "{}")
    except json.JSONDecodeError as exc:
        raise DcmonProbeError(f"docker compose config returned invalid JSON: {exc}") from exc

    services = payload.get("services")
    if not isinstance(services, dict):
        raise DcmonProbeError("docker compose config did not include services")

    service_mount_specs: dict[str, dict[str, str]] = {}
    for service_name, service_config in services.items():
        if not isinstance(service_name, str) or not isinstance(service_config, dict):
            continue
        by_target: dict[str, str] = {}
        for entry in service_config.get("volumes") or []:
            if not isinstance(entry, dict):
                continue
            target = str(entry.get("target") or "")
            spec = compose_config_mount_spec(entry)
            if target and spec:
                by_target[target] = spec
        service_mount_specs[service_name] = by_target
    return service_mount_specs


def build_service_switch_plan(service: ServiceInfo, ticket_token: str, workspace: WorkspaceIndex) -> ServiceSwitchPlan:
    durable_files = durable_compose_config_files(service.compose_config_files)
    mount_plans: list[MountSwitchPlan] = []
    service_plan = ServiceSwitchPlan(
        ticket_token=ticket_token,
        service=service,
        durable_compose_files=durable_files,
        mount_plans=mount_plans,
    )

    for mount in unique_service_mounts(service):
        if mount.kind != "bind":
            mount_plans.append(
                MountSwitchPlan(
                    mount=mount,
                    current_source=mount.source,
                    planned_source=mount.source,
                    resolution="keep",
                    note="keep",
                    changed=False,
                )
            )
            continue

        family, repo_root = resolve_mount_family(mount, workspace)
        if family is None or not mount.source or not repo_root:
            mount_plans.append(
                MountSwitchPlan(
                    mount=mount,
                    current_source=mount.source,
                    planned_source=mount.source,
                    resolution="keep",
                    note="keep",
                    changed=False,
                )
            )
            continue

        checkout_path, resolution = choose_checkout_for_ticket(family, ticket_token)
        planned_source = rebase_mount_source(mount.source, repo_root, checkout_path)
        if not Path(planned_source).exists():
            service_plan.skipped_reason = f"missing path {format_path(planned_source)}"
            return service_plan

        mount_plans.append(
            MountSwitchPlan(
                mount=mount,
                current_source=mount.source,
                planned_source=planned_source,
                resolution=resolution,
                note="ticket" if resolution == "ticket" else "base",
                changed=planned_source != mount.source,
            )
        )

    if any(mount_plan.changed for mount_plan in mount_plans) and not durable_files:
        service_plan.skipped_reason = "no durable compose config files found"
    return service_plan


def build_switch_plan(ticket_token: str, services: Sequence[ServiceInfo], workspace: WorkspaceIndex) -> SwitchPlan:
    normalized_token = ticket_token.upper()
    service_plans: list[ServiceSwitchPlan] = []
    for service in sorted(services, key=service_sort_key):
        service_plan = build_service_switch_plan(service, normalized_token, workspace)
        if service_plan.can_apply() or service_plan.skipped_reason:
            service_plans.append(service_plan)
    return SwitchPlan(ticket_token=normalized_token, service_plans=service_plans)


def count_ticket_matches(ticket_token: str, services: Sequence[ServiceInfo], workspace: WorkspaceIndex) -> int:
    count = 0
    for service in services:
        for mount in unique_service_mounts(service):
            if mount.kind != "bind" or not mount.source:
                continue
            family, repo_root = resolve_mount_family(mount, workspace)
            if family is None or not repo_root:
                continue
            checkout_path, resolution = choose_checkout_for_ticket(family, ticket_token)
            if resolution != "ticket":
                continue
            planned_source = rebase_mount_source(mount.source, repo_root, checkout_path)
            if planned_source and Path(planned_source).exists():
                count += 1
    return count


def matching_repo_names(ticket_token: str, services: Sequence[ServiceInfo], workspace: WorkspaceIndex) -> tuple[str, ...]:
    repo_names: set[str] = set()
    del services
    for checkout in workspace.tokens.get(ticket_token, []):
        family = workspace.families_by_common_dir.get(checkout.common_dir)
        if family is None:
            continue
        repo_names.add(family_display_name(family))
    return tuple(sorted(repo_names))


def active_repo_names(ticket_token: str, services: Sequence[ServiceInfo], workspace: WorkspaceIndex) -> tuple[str, ...]:
    family_common_dirs = {
        checkout.common_dir
        for checkout in workspace.tokens.get(ticket_token, [])
    }
    repo_names: set[str] = set()
    for service in services:
        for mount in unique_service_mounts(service):
            if mount.kind != "bind":
                continue
            family, _repo_root = resolve_mount_family(mount, workspace)
            if family is None or family.common_dir not in family_common_dirs:
                continue
            repo_names.add(family_display_name(family))
    return tuple(sorted(repo_names))


def no_change_status_for_ticket(ticket_token: str, services: Sequence[ServiceInfo], workspace: WorkspaceIndex) -> str:
    discovered_repo_names = matching_repo_names(ticket_token, services, workspace)
    active_names = active_repo_names(ticket_token, services, workspace)
    if active_names:
        return f"{ticket_token}: already active for {', '.join(active_names)}; no changes done"
    if discovered_repo_names:
        return f"{ticket_token}: no active services for {', '.join(discovered_repo_names)}; no changes done"
    return f"{ticket_token}: no running services need changes"


def visible_status_banner(
    last_status: Optional[str],
    last_status_level: str,
    last_error: Optional[str],
) -> tuple[Optional[str], str]:
    if last_error:
        return (f"error: {last_error}", "error")
    if last_status and last_status_level in {"warning", "error"}:
        return (last_status, last_status_level)
    return (None, "info")


def subtitle_status_fragment(last_status: Optional[str], last_status_level: str) -> Optional[str]:
    if not last_status or last_status_level != "info":
        return None
    return last_status


def is_transient_status_level(level: str) -> bool:
    return level in {"warning", "error"}


def build_compose_logs_command(service: ServiceInfo, tail_lines: int = LOG_TAIL_LINES) -> tuple[list[str], Optional[str]]:
    durable_files = durable_compose_config_files(service.compose_config_files)
    if not durable_files:
        raise DcmonProbeError(f"{service.project_name}/{service.service_name}: no durable compose config files found")
    cmd = ["docker", "compose", "--project-name", service.project_name]
    for config_file in durable_files:
        cmd.extend(["-f", config_file])
    cmd.extend(["logs", "--tail", str(tail_lines), "--timestamps", "--no-color", service.service_name])
    return cmd, service.compose_workdir or None


def build_container_logs_commands(service: ServiceInfo, tail_lines: int = LOG_TAIL_LINES) -> list[tuple[str, list[str]]]:
    commands: list[tuple[str, list[str]]] = []
    for container in sorted(service.containers, key=lambda item: item.name):
        commands.append(
            (
                container.name,
                ["docker", "logs", "--tail", str(tail_lines), "--timestamps", container.name],
            )
        )
    return commands


def fetch_service_logs(service: ServiceInfo, tail_lines: int = LOG_TAIL_LINES) -> str:
    try:
        cmd, cwd = build_compose_logs_command(service, tail_lines)
    except DcmonProbeError:
        commands = build_container_logs_commands(service, tail_lines)
        if not commands:
            raise DcmonProbeError(f"{service.project_name}/{service.service_name}: no containers available for logs")
        chunks: list[str] = []
        multiple = len(commands) > 1
        for container_name, cmd in commands:
            output = _run_combined(cmd, timeout=LOG_TIMEOUT)
            normalized = output.rstrip()
            if not normalized:
                continue
            if multiple:
                prefixed = "\n".join(
                    f"{container_name} | {line}" if line else f"{container_name} |"
                    for line in normalized.splitlines()
                )
                chunks.append(prefixed)
            else:
                chunks.append(normalized)
        return "\n\n".join(chunk for chunk in chunks if chunk)
    return _run_combined(cmd, timeout=LOG_TIMEOUT, cwd=cwd).rstrip()


def log_view_is_at_bottom(scroll_y: float, max_scroll_y: float, *, tolerance: float = 0.5) -> bool:
    return max_scroll_y <= 0 or scroll_y >= max_scroll_y - tolerance


def build_ticket_options(services: Sequence[ServiceInfo], workspace: WorkspaceIndex) -> list[TicketOption]:
    options = [
        TicketOption(
            token=token,
            match_count=count_ticket_matches(token, services, workspace),
            repo_names=matching_repo_names(token, services, workspace),
        )
        for token in workspace.tokens
    ]
    return sorted(options, key=lambda option: (-option.match_count, option.token))


def render_compose_override(
    service_plans: Sequence[ServiceSwitchPlan],
    compose_service_mount_specs: Optional[dict[str, dict[str, str]]] = None,
) -> str:
    lines = ["services:"]
    for service_plan in sorted(service_plans, key=lambda item: item.service_name):
        lines.append(f"  {service_plan.service_name}:")
        volume_specs: list[str] = []
        tmpfs_targets: list[str] = []
        config_mount_specs = (compose_service_mount_specs or {}).get(service_plan.service_name, {})
        for mount_plan in service_plan.mount_plans:
            if mount_plan.mount.kind == "tmpfs":
                tmpfs_targets.append(mount_plan.mount.target)
                continue
            if mount_plan.mount.kind == "bind" and mount_plan.changed and mount_plan.planned_source:
                spec = compose_mount_spec(mount_plan.mount, mount_plan.planned_source)
            else:
                spec = config_mount_specs.get(mount_plan.mount.target) or compose_mount_spec(mount_plan.mount)
            if spec:
                volume_specs.append(spec)

        if volume_specs:
            lines.append("    volumes:")
            for spec in volume_specs:
                lines.append(f"      - {json.dumps(spec)}")
        if tmpfs_targets:
            lines.append("    tmpfs:")
            for target in tmpfs_targets:
                lines.append(f"      - {json.dumps(target)}")
    return "\n".join(lines) + "\n"


def group_service_plans_by_compose(service_plans: Sequence[ServiceSwitchPlan]) -> dict[tuple[str, str, tuple[str, ...]], list[ServiceSwitchPlan]]:
    grouped: dict[tuple[str, str, tuple[str, ...]], list[ServiceSwitchPlan]] = {}
    for service_plan in service_plans:
        compose_workdir = service_plan.compose_workdir or ""
        key = (service_plan.project_name, compose_workdir, service_plan.durable_compose_files)
        grouped.setdefault(key, []).append(service_plan)
    return grouped


def verify_switch_plan(plan: SwitchPlan, refreshed_services: Sequence[ServiceInfo]) -> list[str]:
    errors: list[str] = []
    service_map = {service.key: service for service in refreshed_services}
    for service_plan in plan.executable_service_plans():
        if service_plan.apply_error:
            continue
        refreshed_service = service_map.get(service_plan.service.key)
        if refreshed_service is None:
            errors.append(f"{service_plan.service_label}: service not found after refresh")
            continue
        actual_mounts = {
            mount.target: mount
            for mount in unique_service_mounts(refreshed_service)
            if mount.kind == "bind"
        }
        for mount_plan in service_plan.changed_git_mounts():
            actual_mount = actual_mounts.get(mount_plan.mount.target)
            if actual_mount is None:
                errors.append(f"{service_plan.service_label}: missing mount {mount_plan.mount.target}")
                continue
            if actual_mount.source != mount_plan.planned_source:
                errors.append(
                    f"{service_plan.service_label}: {mount_plan.mount.target} is {format_path(actual_mount.source)}"
                    f", expected {format_path(mount_plan.planned_source)}"
                )
    return errors


def execute_switch_plan(plan: SwitchPlan) -> SwitchResult:
    result = SwitchResult()

    for service_plan in plan.service_plans:
        if service_plan.skipped_reason:
            result.skipped_services.append(f"{service_plan.service_label}: {service_plan.skipped_reason}")

    grouped = group_service_plans_by_compose(plan.executable_service_plans())
    for (project_name, compose_workdir, config_files), group in sorted(grouped.items()):
        override_path: Optional[str] = None
        try:
            compose_service_mount_specs = load_compose_service_mount_specs(project_name, compose_workdir, config_files)
            with tempfile.NamedTemporaryFile(
                mode="w",
                prefix=f"{WORKTREE_OVERRIDE_PREFIX}.",
                suffix=".yml",
                dir=str(WORKTREE_OVERRIDE_DIR),
                delete=False,
            ) as handle:
                override_path = handle.name
                handle.write(render_compose_override(group, compose_service_mount_specs))

            cmd = ["docker", "compose", "--project-name", project_name]
            for config_file in config_files:
                cmd.extend(["-f", config_file])
            cmd.extend(["-f", override_path, "up", "-d", "--no-deps", "--force-recreate"])
            cmd.extend(sorted(service_plan.service_name for service_plan in group))
            _run(cmd, timeout=SWITCH_TIMEOUT, cwd=compose_workdir or None)

            for service_plan in group:
                result.applied_services.append(service_plan.service_label)
        except DcmonProbeError as exc:
            message = str(exc)
            result.apply_errors.append(f"{project_name}: {message}")
            for service_plan in group:
                service_plan.apply_error = message
        finally:
            if override_path:
                try:
                    Path(override_path).unlink()
                except OSError:
                    pass

    refreshed_services, refresh_error = gather_services()
    result.refreshed_services = refreshed_services
    result.refresh_error = refresh_error
    if refreshed_services is not None:
        result.verification_errors = verify_switch_plan(plan, refreshed_services)
    return result


def preview_rows_for_plan(plan: SwitchPlan) -> list[tuple[str, str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str, str]] = []
    for service_plan in plan.service_plans:
        if service_plan.skipped_reason:
            rows.append(
                (
                    service_plan.project_name,
                    service_plan.service_name,
                    "-",
                    "-",
                    "-",
                    f"skip: {service_plan.skipped_reason}",
                )
            )
            continue
        for mount_plan in service_plan.changed_git_mounts():
            rows.append(
                (
                    service_plan.project_name,
                    service_plan.service_name,
                    mount_plan.mount.target,
                    format_path(mount_plan.current_source),
                    format_path(mount_plan.planned_source),
                    mount_plan.note,
                )
            )
    return rows


def preview_summary_for_plan(plan: SwitchPlan) -> str:
    if not plan.service_plans:
        return f"{plan.ticket_token}: no running services need changes"

    parts = [f"{plan.ticket_token}"]
    changed_mounts = plan.changed_mount_count()
    if changed_mounts:
        parts.append(f"{changed_mounts} mount(s)")
    fallback_mounts = plan.fallback_mount_count()
    if fallback_mounts:
        parts.append(f"{fallback_mounts} base fallback")
    skipped_services = plan.skipped_service_count()
    if skipped_services:
        parts.append(f"{skipped_services} skipped")
    return " | ".join(parts)


def text_matches_filter(values: Sequence[str], query: str) -> bool:
    normalized_query = query.strip().lower()
    if not normalized_query:
        return True
    haystack = " ".join(values).lower()
    return normalized_query in haystack


def filter_ticket_options(options: Sequence[TicketOption], query: str) -> list[TicketOption]:
    return [
        option
        for option in options
        if text_matches_filter((option.token, ", ".join(option.repo_names)), query)
    ]


def filter_preview_rows(rows: Sequence[tuple[str, str, str, str, str, str]], query: str) -> list[tuple[str, str, str, str, str, str]]:
    return [row for row in rows if text_matches_filter(row, query)]


def run_app() -> None:
    from rich.text import Text
    from textual import work
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.screen import ModalScreen
    from textual.widgets import DataTable, Footer, Header, Input, Static

    class TicketPickerScreen(ModalScreen[Optional[str]]):
        CSS = """
        TicketPickerScreen {
          align: center middle;
        }

        #ticket-picker-title {
          width: 110;
          background: $surface;
          padding: 1 2 0 2;
          border: round $accent;
          border-bottom: none;
        }

        #ticket-picker-table {
          width: 110;
          height: 19;
          background: $surface;
          padding: 0 2 1 2;
          border: round $accent;
          border-top: none;
          border-bottom: none;
        }

        #ticket-picker-filter {
          width: 110;
          height: 3;
          background: $surface;
          padding: 0 2 0 2;
          border: round $accent;
          border-top: none;
        }
        """
        BINDINGS = [
            Binding("escape", "cancel", "Cancel", priority=True),
            Binding("q", "cancel", "Cancel", priority=True),
            Binding("ctrl+c", "cancel", "Cancel", priority=True),
            Binding("enter", "confirm", "Choose", priority=True),
            Binding("j", "cursor_down", "Down", priority=True),
            Binding("k", "cursor_up", "Up", priority=True),
            Binding("g", "cursor_top", "Top", priority=True),
            Binding("G", "cursor_bottom", "Bottom", priority=True),
            Binding("/", "focus_filter", "Filter", priority=True),
        ]

        def __init__(self, options: Sequence[TicketOption]) -> None:
            super().__init__()
            self.options = list(options)
            self.selected_token: Optional[str] = None
            self.filter_query = ""

        def compose(self) -> ComposeResult:
            yield Static("Pick a shared ticket token", id="ticket-picker-title")
            yield DataTable(id="ticket-picker-table")
            yield Input(placeholder="Press / to filter, Enter to apply", id="ticket-picker-filter")

        def on_mount(self) -> None:
            filter_input = self.query_one("#ticket-picker-filter", Input)
            filter_input.value = self.filter_query
            table = self.query_one("#ticket-picker-table", DataTable)
            table.cursor_type = "row"
            table.add_columns("Ticket", "Repos")
            self.render_filtered_table()
            table.focus()

        def action_cancel(self) -> None:
            self.dismiss(None)

        def action_confirm(self) -> None:
            if self.focused is self.query_one("#ticket-picker-filter", Input):
                self.apply_filter()
                return
            if self.selected_token is None and self.options:
                self.selected_token = self.options[0].token
            self.dismiss(self.selected_token)

        def action_cursor_down(self) -> None:
            self._move_cursor(1)

        def action_cursor_up(self) -> None:
            self._move_cursor(-1)

        def action_cursor_top(self) -> None:
            table = self.query_one(DataTable)
            if table.row_count:
                table.move_cursor(row=0, animate=False)

        def action_cursor_bottom(self) -> None:
            table = self.query_one(DataTable)
            if table.row_count:
                table.move_cursor(row=table.row_count - 1, animate=False)

        def action_focus_filter(self) -> None:
            filter_input = self.query_one("#ticket-picker-filter", Input)
            filter_input.focus()
            filter_input.cursor_position = len(filter_input.value)

        def _move_cursor(self, offset: int) -> None:
            table = self.query_one(DataTable)
            if table.row_count == 0:
                return
            current_row = getattr(table.cursor_coordinate, "row", 0)
            bounded_row = max(0, min(table.row_count - 1, current_row + offset))
            table.move_cursor(row=bounded_row, animate=False)

        def render_filtered_table(self) -> None:
            table = self.query_one("#ticket-picker-table", DataTable)
            table.clear()
            visible_options = filter_ticket_options(self.options, self.filter_query)
            self.selected_token = None
            if not visible_options:
                table.add_row("(no matches)", "", key="__empty__")
                table.move_cursor(row=0, animate=False)
                return
            for option in visible_options:
                repo_label = ", ".join(option.repo_names) if option.repo_names else "-"
                table.add_row(option.token, repo_label, key=option.token)
            table.move_cursor(row=0, animate=False)
            self.selected_token = visible_options[0].token

        def apply_filter(self) -> None:
            filter_input = self.query_one("#ticket-picker-filter", Input)
            self.filter_query = filter_input.value
            self.render_filtered_table()
            self.query_one("#ticket-picker-table", DataTable).focus()

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
            key = row_key_value(event.row_key)
            self.selected_token = None if key == "__empty__" else key

        def on_input_submitted(self, event: Input.Submitted) -> None:
            if event.input.id == "ticket-picker-filter":
                self.apply_filter()

    class SwitchPreviewScreen(ModalScreen[bool]):
        CSS = """
        SwitchPreviewScreen {
          align: center middle;
        }

        #switch-preview-summary {
          width: 120;
          background: $surface;
          padding: 1 2 0 2;
          border: round $accent;
          border-bottom: none;
        }

        #switch-preview-table {
          width: 120;
          height: 25;
          background: $surface;
          padding: 0 2 1 2;
          border: round $accent;
          border-top: none;
          border-bottom: none;
        }

        #switch-preview-filter {
          width: 120;
          height: 3;
          background: $surface;
          padding: 0 2 0 2;
          border: round $accent;
          border-top: none;
        }
        """
        BINDINGS = [
            Binding("escape", "cancel", "Cancel", priority=True),
            Binding("q", "cancel", "Cancel", priority=True),
            Binding("ctrl+c", "cancel", "Cancel", priority=True),
            Binding("enter", "confirm", "Apply", priority=True),
            Binding("j", "cursor_down", "Down", priority=True),
            Binding("k", "cursor_up", "Up", priority=True),
            Binding("g", "cursor_top", "Top", priority=True),
            Binding("G", "cursor_bottom", "Bottom", priority=True),
            Binding("/", "focus_filter", "Filter", priority=True),
        ]

        def __init__(self, plan: SwitchPlan) -> None:
            super().__init__()
            self.plan = plan
            self.filter_query = ""
            self.rows = preview_rows_for_plan(self.plan)

        def compose(self) -> ComposeResult:
            yield Static("", id="switch-preview-summary")
            yield DataTable(id="switch-preview-table")
            yield Input(placeholder="Press / to filter, Enter to apply", id="switch-preview-filter")

        def on_mount(self) -> None:
            summary = self.query_one("#switch-preview-summary", Static)
            summary.update(preview_summary_for_plan(self.plan))
            filter_input = self.query_one("#switch-preview-filter", Input)
            filter_input.value = self.filter_query
            table = self.query_one("#switch-preview-table", DataTable)
            table.cursor_type = "row"
            table.add_columns("Project", "Service", "Target", "Current", "Planned", "Mode")
            self.render_filtered_rows()
            table.focus()

        def action_cancel(self) -> None:
            self.dismiss(False)

        def action_confirm(self) -> None:
            if self.focused is self.query_one("#switch-preview-filter", Input):
                self.apply_filter()
                return
            self.dismiss(True)

        def action_cursor_down(self) -> None:
            self._move_cursor(1)

        def action_cursor_up(self) -> None:
            self._move_cursor(-1)

        def action_cursor_top(self) -> None:
            table = self.query_one(DataTable)
            if table.row_count:
                table.move_cursor(row=0, animate=False)

        def action_cursor_bottom(self) -> None:
            table = self.query_one(DataTable)
            if table.row_count:
                table.move_cursor(row=table.row_count - 1, animate=False)

        def action_focus_filter(self) -> None:
            filter_input = self.query_one("#switch-preview-filter", Input)
            filter_input.focus()
            filter_input.cursor_position = len(filter_input.value)

        def _move_cursor(self, offset: int) -> None:
            table = self.query_one(DataTable)
            if table.row_count == 0:
                return
            current_row = getattr(table.cursor_coordinate, "row", 0)
            bounded_row = max(0, min(table.row_count - 1, current_row + offset))
            table.move_cursor(row=bounded_row, animate=False)

        def render_filtered_rows(self) -> None:
            table = self.query_one("#switch-preview-table", DataTable)
            table.clear()
            visible_rows = filter_preview_rows(self.rows, self.filter_query)
            if not visible_rows:
                table.add_row("(no matches)", "", "", "", "", "", key="__empty__")
                table.move_cursor(row=0, animate=False)
                return
            for row in visible_rows:
                table.add_row(*row)
            table.move_cursor(row=0, animate=False)

        def apply_filter(self) -> None:
            filter_input = self.query_one("#switch-preview-filter", Input)
            self.filter_query = filter_input.value
            self.render_filtered_rows()
            self.query_one("#switch-preview-table", DataTable).focus()

        def on_input_submitted(self, event: Input.Submitted) -> None:
            if event.input.id == "switch-preview-filter":
                self.apply_filter()

    class LogViewerScreen(ModalScreen[None]):
        CSS = """
        LogViewerScreen {
          align: center middle;
        }

        #log-viewer-title {
          width: 140;
          background: $surface;
          padding: 1 2 0 2;
          border: round $accent;
          border-bottom: none;
        }

        #log-viewer-body {
          width: 140;
          height: 30;
          background: $surface;
          padding: 0 2;
          border: round $accent;
          border-top: none;
          border-bottom: none;
          overflow-y: auto;
          text-wrap: nowrap;
        }

        #log-viewer-hint {
          width: 140;
          background: $surface;
          padding: 0 2 1 2;
          border: round $accent;
          border-top: none;
          color: $text-muted;
        }
        """
        BINDINGS = [
            Binding("escape", "close", "Close", priority=True),
            Binding("q", "close", "Close", priority=True),
            Binding("w", "close", "Close", priority=True),
            Binding("ctrl+c", "close", "Close", priority=True),
            Binding("j", "scroll_down", "Down", priority=True),
            Binding("k", "scroll_up", "Up", priority=True),
            Binding("g", "scroll_top", "Top", priority=True),
            Binding("G", "scroll_bottom", "Bottom", priority=True),
        ]

        def __init__(self, service: ServiceInfo) -> None:
            super().__init__()
            self.service = service
            self.follow_logs = True
            self.last_logs = ""
            self.last_error: Optional[str] = None

        def compose(self) -> ComposeResult:
            yield Static("", id="log-viewer-title")
            yield Static("", id="log-viewer-body")
            yield Static("", id="log-viewer-hint")

        def on_mount(self) -> None:
            self.update_title()
            self.update_hint()
            self.refresh_logs()
            self.set_interval(LOG_REFRESH_SECONDS, self.refresh_logs)

        def update_title(self) -> None:
            title = self.query_one("#log-viewer-title", Static)
            mode = "follow" if self.follow_logs else "paused"
            suffix = f" | {mode}"
            if self.last_error:
                suffix += " | error"
            title.update(f"Logs: {self.service.project_name}/{self.service.service_name}{suffix}")

        def update_hint(self) -> None:
            hint = self.query_one("#log-viewer-hint", Static)
            hint.update("w/q/Esc close  j/k scroll  g/G top/bottom")

        def action_close(self) -> None:
            self.dismiss(None)

        def body(self) -> Static:
            return self.query_one("#log-viewer-body", Static)

        def action_scroll_down(self) -> None:
            body = self.body()
            body.scroll_to(y=min(body.max_scroll_y, body.scroll_y + 1), animate=False)
            self.follow_logs = log_view_is_at_bottom(body.scroll_y, body.max_scroll_y)
            self.update_title()

        def action_scroll_up(self) -> None:
            body = self.body()
            body.scroll_to(y=max(0, body.scroll_y - 1), animate=False)
            self.follow_logs = log_view_is_at_bottom(body.scroll_y, body.max_scroll_y)
            self.update_title()

        def action_scroll_top(self) -> None:
            body = self.body()
            body.scroll_home(animate=False)
            self.follow_logs = False
            self.update_title()

        def action_scroll_bottom(self) -> None:
            body = self.body()
            body.scroll_end(animate=False)
            self.follow_logs = True
            self.update_title()

        def render_logs(self, content: str) -> None:
            body = self.body()
            previous_scroll_y = body.scroll_y
            was_following = self.follow_logs or log_view_is_at_bottom(body.scroll_y, body.max_scroll_y)
            body.update(content or "(no logs)")
            if was_following:
                body.scroll_end(animate=False)
                self.follow_logs = True
            else:
                body.scroll_to(y=previous_scroll_y, animate=False)
                self.follow_logs = log_view_is_at_bottom(body.scroll_y, body.max_scroll_y)
            self.update_title()

        @work(thread=True, exclusive=True, group="dcmon-logs")
        def refresh_logs(self) -> None:
            try:
                logs = fetch_service_logs(self.service)
                dispatch_app_callback(self.app, self.apply_logs, logs, None)
            except Exception as exc:  # pragma: no cover - UI defensive path
                dispatch_app_callback(self.app, self.apply_logs, None, str(exc))

        def apply_logs(self, logs: Optional[str], error: Optional[str]) -> None:
            if not self.is_attached:
                return
            if error:
                self.last_error = error
                self.render_logs(f"(log fetch failed)\n\n{error}")
                return
            normalized_logs = logs or ""
            self.last_error = None
            if normalized_logs == self.last_logs:
                self.update_title()
                return
            self.last_logs = normalized_logs
            self.render_logs(normalized_logs)

    class DcmonApp(App):
        TITLE = "dcmon"
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
        }

        #status-banner.-hidden {
          display: none;
        }

        #status-banner.-warning {
          background: #5a3b1f;
          color: #ffd7a3;
        }

        #status-banner.-error {
          background: #5a1f1f;
          color: #ffb0b0;
        }
        """
        BINDINGS = [
            ("q", "quit", "Quit"),
            ("j", "cursor_down_vim", "Down"),
            ("k", "cursor_up_vim", "Up"),
            ("g", "cursor_top_vim", "Top"),
            ("G", "cursor_bottom_vim", "Bottom"),
            ("l", "view_logs", "Logs"),
            ("s", "switch_ticket", "Switch"),
        ]

        def __init__(self) -> None:
            super().__init__()
            self.services: list[ServiceInfo] = []
            self.selected_key: Optional[str] = None
            self.last_error: Optional[str] = None
            self.last_status: Optional[str] = None
            self.last_status_level = "info"
            self.status_generation = 0
            self.last_successful_refresh: Optional[str] = None
            self.service_row_indices: list[int] = []
            self.switch_busy = False

        def compose(self) -> ComposeResult:
            yield Header()
            yield DataTable(id="services")
            yield Static(id="details")
            yield Static("", id="status-banner", classes="-hidden")
            yield Footer()

        def on_mount(self) -> None:
            table = self.query_one("#services", DataTable)
            table.cursor_type = "row"
            table.zebra_stripes = True
            table.add_columns("Project", "Service", "Repo", "Branch", "WT", "Extra")
            self.update_sub_title()
            self.update_status_banner()
            self.refresh_snapshot()
            self.set_interval(REFRESH_SECONDS, self.refresh_snapshot)

        def update_sub_title(self) -> None:
            parts = [f"refresh {int(REFRESH_SECONDS)}s"]
            if self.last_successful_refresh:
                parts.append(f"last ok {self.last_successful_refresh}")
            status_fragment = subtitle_status_fragment(self.last_status, self.last_status_level)
            if status_fragment:
                status = status_fragment
                if len(status) > 96:
                    status = status[:93] + "..."
                parts.append(status)
            if self.last_error:
                error = self.last_error
                if len(error) > 80:
                    error = error[:77] + "..."
                parts.append(f"error {error}")
            self.sub_title = " | ".join(parts)

        def update_status_banner(self) -> None:
            banner = self.query_one("#status-banner", Static)
            message, level = visible_status_banner(self.last_status, self.last_status_level, self.last_error)
            banner.remove_class("-hidden", "-warning", "-error")
            if not message:
                banner.update("")
                banner.add_class("-hidden")
                return
            display = message if len(message) <= 160 else message[:157] + "..."
            banner.update(display)
            banner.add_class(f"-{level}")

        def set_status(self, message: Optional[str], level: str = "info") -> None:
            self.status_generation += 1
            self.last_status = message
            self.last_status_level = level
            self.update_sub_title()
            self.update_status_banner()
            if message and is_transient_status_level(level):
                generation = self.status_generation
                self.set_timer(STATUS_BANNER_TIMEOUT, lambda: self.expire_status(generation))

        def expire_status(self, generation: int) -> None:
            if generation != self.status_generation:
                return
            if not is_transient_status_level(self.last_status_level):
                return
            self.last_status = None
            self.last_status_level = "info"
            self.update_sub_title()
            self.update_status_banner()

        def refresh_snapshot(self) -> None:
            self.collect_snapshot()

        @work(thread=True, exclusive=True, group="dcmon-refresh")
        def collect_snapshot(self) -> None:
            services, error = gather_services()
            self.call_from_thread(self.apply_snapshot, services, error)

        def apply_snapshot(self, services: Optional[list[ServiceInfo]], error: Optional[str]) -> None:
            if services is not None:
                self.services = services
                self.last_successful_refresh = datetime.now().strftime("%H:%M:%S")
            self.last_error = error
            self.update_sub_title()
            self.update_status_banner()
            self.render_table()
            self.render_details()

        def render_table(self) -> None:
            table = self.query_one("#services", DataTable)
            current_key = self.selected_key
            table.clear()
            self.service_row_indices = []

            if not self.services:
                table.add_row("(no running compose services)", "", "", "", "", "", key="__empty__")
                table.move_cursor(row=0, animate=False)
                self.selected_key = None
                return

            visible_project = None
            selected_row_index: Optional[int] = None
            ordered_services = sorted(self.services, key=service_sort_key)

            for index, service in enumerate(ordered_services):
                key = f"{service.project_name}|{service.service_name}"
                project_label = service.project_name if service.project_name != visible_project else ""
                visible_project = service.project_name
                extra_label = f"+{service.extra_git_mounts}" if service.extra_git_mounts else ""
                row_style = row_style_for_service(service)

                def cell(value: str) -> object:
                    if row_style:
                        return Text(value, style=row_style)
                    return value

                table.add_row(
                    cell(project_label),
                    cell(service.service_name),
                    cell(service.primary_repo_name),
                    cell(service.primary_branch),
                    cell(format_worktree_flag(service.primary_is_worktree)),
                    cell(extra_label),
                    key=key,
                )
                self.service_row_indices.append(index)
                if current_key == key:
                    selected_row_index = index

            if selected_row_index is None:
                selected_row_index = self.service_row_indices[0]
            table.move_cursor(row=selected_row_index, animate=False)

        def move_cursor_to_row(self, row_index: int) -> None:
            table = self.query_one("#services", DataTable)
            if table.row_count == 0:
                return
            bounded_row = max(0, min(table.row_count - 1, row_index))
            table.move_cursor(row=bounded_row, animate=False)

        def move_cursor_to_service_index(self, offset: int) -> None:
            if not self.service_row_indices:
                return
            table = self.query_one("#services", DataTable)
            current_row = getattr(table.cursor_coordinate, "row", 0)
            target_index = 0
            for index, service_row in enumerate(self.service_row_indices):
                if service_row >= current_row:
                    target_index = index
                    break
            else:
                target_index = len(self.service_row_indices) - 1

            if current_row in self.service_row_indices:
                target_index = self.service_row_indices.index(current_row)

            target_index = max(0, min(len(self.service_row_indices) - 1, target_index + offset))
            self.move_cursor_to_row(self.service_row_indices[target_index])

        def action_cursor_down_vim(self) -> None:
            self.move_cursor_to_service_index(1)

        def action_cursor_up_vim(self) -> None:
            self.move_cursor_to_service_index(-1)

        def action_cursor_top_vim(self) -> None:
            if self.service_row_indices:
                self.move_cursor_to_row(self.service_row_indices[0])

        def action_cursor_bottom_vim(self) -> None:
            if self.service_row_indices:
                self.move_cursor_to_row(self.service_row_indices[-1])

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
            key = row_key_value(event.row_key)
            if key == "__empty__":
                self.selected_key = None
            else:
                self.selected_key = key
            self.render_details()

        def selected_service(self) -> Optional[ServiceInfo]:
            if not self.services:
                return None
            if self.selected_key:
                for service in self.services:
                    if self.selected_key == f"{service.project_name}|{service.service_name}":
                        return service
            return self.services[0]

        def render_details(self) -> None:
            details = self.query_one("#details", Static)
            details.update(render_detail_text(self.selected_service(), self.last_error))

        def action_view_logs(self) -> None:
            service = self.selected_service()
            if service is None:
                self.set_status("no service selected", "warning")
                return
            self.push_screen(LogViewerScreen(service))

        def action_switch_ticket(self) -> None:
            if self.switch_busy:
                return
            self.switch_busy = True
            self.set_status("scanning worktrees")
            self.load_ticket_picker()

        @work(thread=True, exclusive=True, group="dcmon-switch")
        def load_ticket_picker(self) -> None:
            try:
                workspace = scan_workspace_index(self.services)
                options = build_ticket_options(self.services, workspace)
                self.call_from_thread(self.present_ticket_picker, workspace, options, None)
            except Exception as exc:  # pragma: no cover - UI defensive path
                self.call_from_thread(self.present_ticket_picker, None, None, str(exc))

        def present_ticket_picker(
            self,
            workspace: Optional[WorkspaceIndex],
            options: Optional[list[TicketOption]],
            error: Optional[str],
        ) -> None:
            self.switch_busy = False
            if error:
                self.set_status(f"switch failed: {error}", "error")
                return
            if workspace is None or options is None or not options:
                self.set_status("no ticket worktrees found", "warning")
                return
            self.set_status(f"tickets {len(options)}")
            self.push_screen(
                TicketPickerScreen(options),
                callback=lambda token: self.handle_ticket_pick(workspace, token),
            )

        def handle_ticket_pick(self, workspace: WorkspaceIndex, token: Optional[str]) -> None:
            if not token:
                self.set_status("switch cancelled")
                return
            if self.switch_busy:
                return
            self.switch_busy = True
            self.set_status(f"planning {token}")
            self.load_switch_preview(workspace, token)

        @work(thread=True, exclusive=True, group="dcmon-switch")
        def load_switch_preview(self, workspace: WorkspaceIndex, token: str) -> None:
            try:
                plan = build_switch_plan(token, self.services, workspace)
                self.call_from_thread(self.present_switch_preview, plan, workspace, None)
            except Exception as exc:  # pragma: no cover - UI defensive path
                self.call_from_thread(self.present_switch_preview, None, workspace, str(exc))

        def present_switch_preview(
            self,
            plan: Optional[SwitchPlan],
            workspace: WorkspaceIndex,
            error: Optional[str],
        ) -> None:
            self.switch_busy = False
            if error:
                self.set_status(f"switch failed: {error}", "error")
                return
            if plan is None:
                self.set_status("switch planning failed", "error")
                return
            if not plan.service_plans:
                self.set_status(no_change_status_for_ticket(plan.ticket_token, self.services, workspace), "warning")
                return
            self.set_status(preview_summary_for_plan(plan))
            self.push_screen(
                SwitchPreviewScreen(plan),
                callback=lambda confirmed: self.handle_switch_confirmation(plan, confirmed),
            )

        def handle_switch_confirmation(self, plan: SwitchPlan, confirmed: bool) -> None:
            if not confirmed:
                self.set_status("switch cancelled")
                return
            self.switch_busy = True
            self.set_status(f"applying {plan.ticket_token}")
            self.apply_switch_plan_worker(plan)

        @work(thread=True, exclusive=True, group="dcmon-switch")
        def apply_switch_plan_worker(self, plan: SwitchPlan) -> None:
            try:
                result = execute_switch_plan(plan)
                self.call_from_thread(self.finish_switch, plan, result, None)
            except Exception as exc:  # pragma: no cover - UI defensive path
                self.call_from_thread(self.finish_switch, plan, None, str(exc))

        def finish_switch(
            self,
            plan: SwitchPlan,
            result: Optional[SwitchResult],
            error: Optional[str],
        ) -> None:
            self.switch_busy = False
            if error:
                self.set_status(f"{plan.ticket_token}: {error}", "error")
                return
            if result is None:
                self.set_status(f"{plan.ticket_token}: switch failed", "error")
                return
            if result.refreshed_services is not None:
                self.services = result.refreshed_services
                self.last_successful_refresh = datetime.now().strftime("%H:%M:%S")
            self.last_error = result.refresh_error
            status_level = "warning" if (result.apply_errors or result.verification_errors or result.skipped_services) else "info"
            self.set_status(result.summary(plan.ticket_token), status_level)
            self.render_table()
            self.render_details()

    DcmonApp().run()


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if args == ["--version"]:
        print(resolve_app_version())
        return 0
    if args:
        print("usage: dcmon [--version]", file=sys.stderr)
        return 2
    run_app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
