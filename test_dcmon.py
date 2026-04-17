#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import dcmon


BACKEND_INSPECT = {
    "Id": "backend-id",
    "Name": "/backend",
    "State": {"Status": "running"},
    "Config": {
        "Labels": {
            "com.docker.compose.project": "sample-stack",
            "com.docker.compose.service": "backend",
            "com.docker.compose.project.working_dir": "/Users/example/src/sample-suite/sample-stack",
            "com.docker.compose.project.config_files": "/Users/example/src/sample-suite/sample-stack/docker-compose.yml",
        }
    },
    "Mounts": [
        {
            "Type": "bind",
            "Source": "/Users/example/src/sample-suite/api-service",
            "Destination": "/app",
        },
        {
            "Type": "volume",
            "Name": "sample-stack-backend-modules",
            "Source": "/var/lib/docker/volumes/sample-stack-backend-modules/_data",
            "Destination": "/app/node_modules",
        },
    ],
}


WEB_CLIENT_INSPECT = {
    "Id": "web-client-id",
    "Name": "/web-client",
    "State": {"Status": "running"},
    "Config": {
        "Labels": {
            "com.docker.compose.project": "sample-stack",
            "com.docker.compose.service": "web-client",
            "com.docker.compose.project.working_dir": "/Users/example/src/sample-suite/sample-stack",
            "com.docker.compose.project.config_files": "/Users/example/src/sample-suite/sample-stack/docker-compose.yml,/tmp/docker-compose.worktree.override.yml",
        }
    },
    "Mounts": [
        {
            "Type": "bind",
            "Source": "/Users/example/src/sample-suite/web-client_PROJ-123",
            "Destination": "/app",
        },
        {
            "Type": "volume",
            "Name": "sample-stack-web-client-modules",
            "Source": "/var/lib/docker/volumes/sample-stack-web-client-modules/_data",
            "Destination": "/app/node_modules",
        },
    ],
}


WORKER_INSPECT = {
    "Id": "worker-id",
    "Name": "/worker",
    "State": {"Status": "running"},
    "Config": {
        "Labels": {
            "com.docker.compose.project": "sample-stack",
            "com.docker.compose.service": "worker",
            "com.docker.compose.project.working_dir": "/Users/example/src/sample-suite/sample-stack",
            "com.docker.compose.project.config_files": "/Users/example/src/sample-suite/sample-stack/docker-compose.yml",
        }
    },
    "Mounts": [
        {
            "Type": "bind",
            "Source": "/Users/example/src/sample-suite/worker-service",
            "Destination": "/app",
        },
        {
            "Type": "bind",
            "Source": "/Users/example/src/sample-suite/shared-lib",
            "Destination": "/shared-lib",
        },
        {
            "Type": "volume",
            "Name": "sample-stack-worker-node-modules",
            "Source": "/var/lib/docker/volumes/sample-stack-worker-node-modules/_data",
            "Destination": "/app/node_modules",
            "RW": False,
        },
        {
            "Type": "tmpfs",
            "Destination": "/tmp/cache",
        },
    ],
}


def make_service(
    *,
    project_name: str = "sample-stack",
    service_name: str,
    compose_file: str,
    mount_source: str,
    branch: str,
    common_dir: str,
    main_repo_path: Optional[str] = None,
    repo_root: Optional[str] = None,
    extra_mounts: Optional[list[dcmon.MountInfo]] = None,
) -> dcmon.ServiceInfo:
    repo_root_value = repo_root or mount_source
    git = dcmon.GitProbeInfo(
        repo_root=repo_root_value,
        repo_name=Path(repo_root_value).name,
        branch=branch,
        is_worktree=main_repo_path is not None,
        main_repo_path=main_repo_path,
        common_dir=common_dir,
        ticket_token=dcmon.extract_ticket_token(branch)
        or dcmon.extract_ticket_token(Path(repo_root_value).name),
    )
    mounts = [dcmon.MountInfo(kind="bind", source=mount_source, target="/app", git=git)]
    if extra_mounts:
        mounts.extend(extra_mounts)

    service = dcmon.ServiceInfo(
        project_name=project_name,
        service_name=service_name,
        compose_workdir=str(Path(compose_file).parent),
        compose_config_files=(compose_file,),
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


class ParseTests(unittest.TestCase):
    def test_list_compose_container_ids(self) -> None:
        with patch.object(dcmon, "_run", return_value="abc\n\ndef\n"):
            self.assertEqual(["abc", "def"], dcmon.list_compose_container_ids())

    def test_inspect_containers_empty_short_circuit(self) -> None:
        with patch.object(dcmon, "_run") as run_mock:
            self.assertEqual([], dcmon.inspect_containers([]))
            run_mock.assert_not_called()

    def test_parse_mounts_bind_volume_tmpfs(self) -> None:
        mounts = dcmon.parse_mounts(WORKER_INSPECT)
        self.assertEqual(
            ["bind", "bind", "volume", "tmpfs"], [mount.kind for mount in mounts]
        )
        self.assertEqual("/app", mounts[0].target)
        self.assertTrue(mounts[2].read_only)
        self.assertEqual("/tmp/cache", mounts[-1].target)

    def test_fallback_workdir_from_config_files(self) -> None:
        path = dcmon.fallback_workdir_from_config_files(
            "/Users/example/src/sample-suite/sample-stack/docker-compose.yml,/tmp/override.yml"
        )
        self.assertEqual("/Users/example/src/sample-suite/sample-stack", path)

    def test_derive_compose_workdir_prefers_label(self) -> None:
        workdir = dcmon.derive_compose_workdir([WEB_CLIENT_INSPECT])
        self.assertEqual("/Users/example/src/sample-suite/sample-stack", workdir)

    def test_durable_compose_config_files_filters_temp_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_file = Path(tmpdir) / "docker-compose.yml"
            base_file.write_text("services:\n")
            override = f"/tmp/{dcmon.WORKTREE_OVERRIDE_PREFIX}.abc123.yml"
            durable = dcmon.durable_compose_config_files((str(base_file), override))
        self.assertEqual((str(base_file),), durable)

    def test_compose_config_mount_spec_supports_named_and_anonymous_volumes(
        self,
    ) -> None:
        self.assertEqual(
            "web-client-modules:/app/node_modules",
            dcmon.compose_config_mount_spec(
                {
                    "type": "volume",
                    "source": "web-client-modules",
                    "target": "/app/node_modules",
                }
            ),
        )
        self.assertEqual(
            "/app/node_modules",
            dcmon.compose_config_mount_spec(
                {"type": "volume", "target": "/app/node_modules"}
            ),
        )

    def test_load_compose_service_mount_specs_reads_compose_json(self) -> None:
        payload = json.dumps(
            {
                "services": {
                    "web-client": {
                        "volumes": [
                            {
                                "type": "bind",
                                "source": "/Users/example/src/sample-suite/web-client",
                                "target": "/app",
                            },
                            {
                                "type": "volume",
                                "source": "web-client-modules",
                                "target": "/app/node_modules",
                            },
                            {"type": "volume", "target": "/cache"},
                        ]
                    }
                }
            }
        )
        with patch.object(dcmon, "_run", return_value=payload) as run_mock:
            specs = dcmon.load_compose_service_mount_specs(
                "sample-stack",
                "/Users/example/src/sample-suite/sample-stack",
                ("/Users/example/src/sample-suite/sample-stack/docker-compose.yml",),
            )

        run_mock.assert_called_once()
        self.assertEqual(
            {
                "web-client": {
                    "/app": "/Users/example/src/sample-suite/web-client:/app",
                    "/app/node_modules": "web-client-modules:/app/node_modules",
                    "/cache": "/cache",
                }
            },
            specs,
        )

    def test_build_compose_logs_command_uses_durable_compose_files(self) -> None:
        service = dcmon.ServiceInfo(
            project_name="sample-stack",
            service_name="web-client",
            compose_workdir="/Users/example/src/sample-suite/sample-stack",
            compose_config_files=(
                "/Users/example/src/sample-suite/sample-stack/docker-compose.yml",
                f"/tmp/{dcmon.WORKTREE_OVERRIDE_PREFIX}.old.yml",
            ),
        )
        with patch.object(
            dcmon,
            "durable_compose_config_files",
            return_value=(
                "/Users/example/src/sample-suite/sample-stack/docker-compose.yml",
            ),
        ):
            cmd, cwd = dcmon.build_compose_logs_command(service, tail_lines=75)

        self.assertEqual(
            [
                "docker",
                "compose",
                "--project-name",
                "sample-stack",
                "-f",
                "/Users/example/src/sample-suite/sample-stack/docker-compose.yml",
                "logs",
                "--tail",
                "75",
                "--timestamps",
                "--no-color",
                "web-client",
            ],
            cmd,
        )
        self.assertEqual("/Users/example/src/sample-suite/sample-stack", cwd)

    def test_fetch_service_logs_prefers_compose_logs(self) -> None:
        service = dcmon.ServiceInfo(
            project_name="sample-stack",
            service_name="web-client",
            compose_workdir="/Users/example/src/sample-suite/sample-stack",
            compose_config_files=(
                "/Users/example/src/sample-suite/sample-stack/docker-compose.yml",
            ),
        )
        with patch.object(
            dcmon,
            "build_compose_logs_command",
            return_value=(["docker", "compose", "logs"], "/tmp"),
        ):
            with patch.object(
                dcmon, "_run_combined", return_value="line 1\nline 2\n"
            ) as run_mock:
                logs = dcmon.fetch_service_logs(service, tail_lines=50)

        self.assertEqual("line 1\nline 2", logs)
        run_mock.assert_called_once_with(
            ["docker", "compose", "logs"], timeout=dcmon.LOG_TIMEOUT, cwd="/tmp"
        )

    def test_fetch_service_logs_falls_back_to_container_logs(self) -> None:
        service = dcmon.ServiceInfo(
            project_name="sample-stack",
            service_name="backend",
            compose_workdir=None,
            containers=[
                dcmon.ContainerInfo(
                    container_id="1",
                    name="backend-1",
                    service="backend",
                    state="running",
                ),
                dcmon.ContainerInfo(
                    container_id="2",
                    name="backend-2",
                    service="backend",
                    state="running",
                ),
            ],
        )

        outputs = {
            "backend-1": "a1\na2\n",
            "backend-2": "b1\n",
        }

        def fake_run(
            cmd: list[str],
            timeout: float = dcmon.DEFAULT_TIMEOUT,
            cwd: Optional[str] = None,
        ) -> str:
            return outputs[cmd[-1]]

        with patch.object(
            dcmon,
            "build_compose_logs_command",
            side_effect=dcmon.DcmonProbeError("no compose"),
        ):
            with patch.object(dcmon, "_run_combined", side_effect=fake_run):
                logs = dcmon.fetch_service_logs(service, tail_lines=10)

        self.assertEqual("backend-1 | a1\nbackend-1 | a2\n\nbackend-2 | b1", logs)


class GitProbeTests(unittest.TestCase):
    def test_probe_git_path_returns_none_when_not_repo(self) -> None:
        with patch.object(
            dcmon, "git_rev_parse", side_effect=dcmon.DcmonProbeError("no repo")
        ):
            self.assertIsNone(dcmon.probe_git_path("/tmp/not-a-repo"))

    def test_probe_git_path_marks_worktree(self) -> None:
        values = iter(
            [
                "/Users/example/src/sample-suite/web-client_PROJ-123",
                "PROJ-123",
                "/Users/example/src/sample-suite/web-client/.git/worktrees/web-client_PROJ-123",
                "/Users/example/src/sample-suite/web-client/.git",
            ]
        )
        with patch.object(
            dcmon, "git_rev_parse", side_effect=lambda path, *args: next(values)
        ):
            info = dcmon.probe_git_path(
                "/Users/example/src/sample-suite/web-client_PROJ-123"
            )

        self.assertIsNotNone(info)
        assert info is not None
        self.assertEqual("web-client_PROJ-123", info.repo_name)
        self.assertEqual("PROJ-123", info.branch)
        self.assertTrue(info.is_worktree)
        self.assertEqual(
            "/Users/example/src/sample-suite/web-client", info.main_repo_path
        )
        self.assertEqual("PROJ-123", info.ticket_token)

    def test_probe_git_path_formats_detached_head(self) -> None:
        values = iter(
            [
                "/Users/example/src/sample-suite/api-service",
                "HEAD",
                "abc1234",
                "/Users/example/src/sample-suite/api-service/.git",
                "/Users/example/src/sample-suite/api-service/.git",
            ]
        )
        with patch.object(
            dcmon, "git_rev_parse", side_effect=lambda path, *args: next(values)
        ):
            info = dcmon.probe_git_path("/Users/example/src/sample-suite/api-service")

        self.assertIsNotNone(info)
        assert info is not None
        self.assertEqual("(detached abc1234)", info.branch)
        self.assertFalse(info.is_worktree)

    def test_extract_ticket_token_variants(self) -> None:
        cases = {
            "PROJ-126-api-service": "PROJ-126",
            "web-client_PROJ-123": "PROJ-123",
            "web-client-PROJ-127": "PROJ-127",
            "staging": None,
            "hotfix/loader_getting_stuck": None,
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(expected, dcmon.extract_ticket_token(raw))


class CliTests(unittest.TestCase):
    def test_resolve_app_version_prefers_env(self) -> None:
        with patch.dict("os.environ", {dcmon.APP_VERSION_ENV: "1.2.3"}, clear=False):
            with patch.object(dcmon, "version_resource_candidates", return_value=()):
                self.assertEqual("1.2.3", dcmon.resolve_app_version())

    def test_resolve_app_version_reads_version_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            version_file = Path(tmpdir) / dcmon.APP_VERSION_RESOURCE
            version_file.write_text("0.4.0\n")

            with patch.dict("os.environ", {}, clear=True):
                with patch.object(
                    dcmon, "version_resource_candidates", return_value=(version_file,)
                ):
                    self.assertEqual("0.4.0", dcmon.resolve_app_version())

    def test_main_prints_version(self) -> None:
        stdout = io.StringIO()
        with patch.object(dcmon, "resolve_app_version", return_value="9.9.9"):
            with patch("sys.stdout", stdout):
                exit_code = dcmon.main(["--version"])

        self.assertEqual(0, exit_code)
        self.assertEqual("9.9.9\n", stdout.getvalue())

    def test_main_rejects_unknown_flags(self) -> None:
        stderr = io.StringIO()
        with patch("sys.stderr", stderr):
            exit_code = dcmon.main(["--bogus"])

        self.assertEqual(2, exit_code)
        self.assertIn("usage: dcmon [--version]", stderr.getvalue())


class StatusTests(unittest.TestCase):
    def test_dispatch_app_callback_uses_app_dispatcher(self) -> None:
        calls: list[tuple[object, tuple[object, ...]]] = []

        class FakeApp:
            def call_from_thread(self, callback: object, *args: object) -> None:
                calls.append((callback, args))

        callback = object()
        dcmon.dispatch_app_callback(FakeApp(), callback, "line 1", None)

        self.assertEqual([(callback, ("line 1", None))], calls)

    def test_dispatch_app_callback_rejects_missing_dispatcher(self) -> None:
        with self.assertRaisesRegex(dcmon.DcmonProbeError, "UI dispatcher unavailable"):
            dcmon.dispatch_app_callback(object(), lambda: None)

    def test_is_transient_status_level(self) -> None:
        self.assertTrue(dcmon.is_transient_status_level("warning"))
        self.assertTrue(dcmon.is_transient_status_level("error"))
        self.assertFalse(dcmon.is_transient_status_level("info"))

    def test_log_view_is_at_bottom(self) -> None:
        self.assertTrue(dcmon.log_view_is_at_bottom(10, 10))
        self.assertTrue(dcmon.log_view_is_at_bottom(9.6, 10))
        self.assertFalse(dcmon.log_view_is_at_bottom(8, 10))

    def test_visible_status_banner_prefers_warning_status(self) -> None:
        message, level = dcmon.visible_status_banner(
            "PROJ-124: no active services for web-client; no changes done",
            "warning",
            None,
        )
        self.assertEqual(
            "PROJ-124: no active services for web-client; no changes done", message
        )
        self.assertEqual("warning", level)

    def test_visible_status_banner_prefers_error(self) -> None:
        message, level = dcmon.visible_status_banner(
            "tickets 10", "info", "docker unavailable"
        )
        self.assertEqual("error: docker unavailable", message)
        self.assertEqual("error", level)

    def test_visible_status_banner_hides_info_status(self) -> None:
        message, level = dcmon.visible_status_banner("planning PROJ-124", "info", None)
        self.assertIsNone(message)
        self.assertEqual("info", level)

    def test_subtitle_status_fragment_only_shows_info(self) -> None:
        self.assertEqual(
            "planning PROJ-124",
            dcmon.subtitle_status_fragment("planning PROJ-124", "info"),
        )
        self.assertIsNone(
            dcmon.subtitle_status_fragment(
                "PROJ-124: no active services for web-client; no changes done",
                "warning",
            )
        )


class WorkspaceTests(unittest.TestCase):
    def test_derive_workspace_scan_roots_uses_active_repo_parent_directories(
        self,
    ) -> None:
        services = [
            make_service(
                service_name="backend",
                compose_file="/tmp/sample-stack/docker-compose.yml",
                mount_source="/Users/example/src/sample-suite/api-service_PROJ-123",
                branch="PROJ-123",
                common_dir="/Users/example/src/sample-suite/api-service/.git",
                main_repo_path="/Users/example/src/sample-suite/api-service",
            ),
            make_service(
                service_name="api",
                compose_file="/tmp/other/docker-compose.yml",
                mount_source="/Users/example/src/example-org/service-api",
                branch="main",
                common_dir="/Users/example/src/example-org/service-api/.git",
            ),
        ]

        roots = dcmon.derive_workspace_scan_roots(services)

        self.assertEqual(
            (
                Path("/Users/example/src/example-org"),
                Path("/Users/example/src/sample-suite"),
            ),
            roots,
        )

    def test_scan_workspace_index_uses_derived_service_roots(self) -> None:
        service = make_service(
            service_name="backend",
            compose_file="/tmp/sample-stack/docker-compose.yml",
            mount_source="/Users/example/src/sample-suite/api-service_PROJ-123",
            branch="PROJ-123",
            common_dir="/Users/example/src/sample-suite/api-service/.git",
            main_repo_path="/Users/example/src/sample-suite/api-service",
        )
        seen_roots: list[Path] = []

        def fake_iter_checkout_roots(root: Path) -> list[Path]:
            seen_roots.append(root)
            return []

        with patch.object(
            dcmon, "iter_checkout_roots", side_effect=fake_iter_checkout_roots
        ):
            index = dcmon.scan_workspace_index([service])

        self.assertEqual([Path("/Users/example/src/sample-suite")], seen_roots)
        self.assertEqual(("/Users/example/src/sample-suite",), index.scan_roots)

    def test_build_workspace_index_groups_repo_families(self) -> None:
        checkouts = [
            dcmon.WorkspaceCheckout(
                checkout_path="/Users/example/src/sample-suite/api-service",
                repo_name="api-service",
                branch="master",
                common_dir="/Users/example/src/sample-suite/api-service/.git",
                is_worktree=False,
                token=None,
                branch_token=None,
            ),
            dcmon.WorkspaceCheckout(
                checkout_path="/Users/example/src/sample-suite/api-service_PROJ-123",
                repo_name="api-service_PROJ-123",
                branch="PROJ-123",
                common_dir="/Users/example/src/sample-suite/api-service/.git",
                is_worktree=True,
                token="PROJ-123",
                branch_token="PROJ-123",
            ),
            dcmon.WorkspaceCheckout(
                checkout_path="/Users/example/src/sample-suite/web-client",
                repo_name="web-client",
                branch="master",
                common_dir="/Users/example/src/sample-suite/web-client/.git",
                is_worktree=False,
                token=None,
                branch_token=None,
            ),
            dcmon.WorkspaceCheckout(
                checkout_path="/Users/example/src/sample-suite/web-client_PROJ-123",
                repo_name="web-client_PROJ-123",
                branch="PROJ-123",
                common_dir="/Users/example/src/sample-suite/web-client/.git",
                is_worktree=True,
                token="PROJ-123",
                branch_token="PROJ-123",
            ),
        ]

        index = dcmon.build_workspace_index(checkouts, Path("/Users/example/src"))
        self.assertEqual(2, len(index.families_by_common_dir))
        self.assertEqual(
            "/Users/example/src/sample-suite/api-service",
            index.families_by_common_dir[
                "/Users/example/src/sample-suite/api-service/.git"
            ].primary_checkout_path,
        )
        self.assertEqual(2, len(index.tokens["PROJ-123"]))

    def test_choose_checkout_for_ticket_prefers_branch_match(self) -> None:
        family = dcmon.RepoFamily(
            common_dir="/Users/example/src/sample-suite/web-client/.git",
            primary_checkout_path="/Users/example/src/sample-suite/web-client",
            checkouts=[
                dcmon.WorkspaceCheckout(
                    checkout_path="/Users/example/src/sample-suite/web-client_feature_copy",
                    repo_name="web-client_feature_copy",
                    branch="feature/no-ticket",
                    common_dir="/Users/example/src/sample-suite/web-client/.git",
                    is_worktree=True,
                    token="PROJ-123",
                    branch_token=None,
                ),
                dcmon.WorkspaceCheckout(
                    checkout_path="/Users/example/src/sample-suite/web-client_PROJ-123",
                    repo_name="web-client_PROJ-123",
                    branch="PROJ-123",
                    common_dir="/Users/example/src/sample-suite/web-client/.git",
                    is_worktree=True,
                    token="PROJ-123",
                    branch_token="PROJ-123",
                ),
            ],
        )

        checkout_path, resolution = dcmon.choose_checkout_for_ticket(family, "PROJ-123")
        self.assertEqual("ticket", resolution)
        self.assertEqual(
            "/Users/example/src/sample-suite/web-client_PROJ-123", checkout_path
        )


class GatherTests(unittest.TestCase):
    def test_build_service_snapshot_summarizes_primary_repo_and_extra_mounts(
        self,
    ) -> None:
        inspect_entries = [BACKEND_INSPECT, WEB_CLIENT_INSPECT, WORKER_INSPECT]
        probes = {
            "/Users/example/src/sample-suite/api-service": dcmon.GitProbeInfo(
                repo_root="/Users/example/src/sample-suite/api-service",
                repo_name="api-service",
                branch="master",
                is_worktree=False,
                common_dir="/Users/example/src/sample-suite/api-service/.git",
            ),
            "/Users/example/src/sample-suite/web-client_PROJ-123": dcmon.GitProbeInfo(
                repo_root="/Users/example/src/sample-suite/web-client_PROJ-123",
                repo_name="web-client_PROJ-123",
                branch="PROJ-123",
                is_worktree=True,
                main_repo_path="/Users/example/src/sample-suite/web-client",
                common_dir="/Users/example/src/sample-suite/web-client/.git",
            ),
            "/Users/example/src/sample-suite/worker-service": dcmon.GitProbeInfo(
                repo_root="/Users/example/src/sample-suite/worker-service",
                repo_name="worker-service",
                branch="master",
                is_worktree=False,
                common_dir="/Users/example/src/sample-suite/worker-service/.git",
            ),
            "/Users/example/src/sample-suite/shared-lib": dcmon.GitProbeInfo(
                repo_root="/Users/example/src/sample-suite/shared-lib",
                repo_name="shared-lib",
                branch="master",
                is_worktree=False,
                common_dir="/Users/example/src/sample-suite/shared-lib/.git",
            ),
        }

        with patch.object(
            dcmon, "probe_git_path", side_effect=lambda source: probes.get(source)
        ):
            services, error = dcmon.build_service_snapshot(inspect_entries)

        self.assertIsNone(error)
        self.assertEqual(
            [
                ("sample-stack", "backend"),
                ("sample-stack", "web-client"),
                ("sample-stack", "worker"),
            ],
            [(service.project_name, service.service_name) for service in services],
        )

        backend = services[0]
        self.assertEqual("api-service", backend.primary_repo_name)
        self.assertEqual("master", backend.primary_branch)
        self.assertEqual(0, backend.extra_git_mounts)
        self.assertEqual(
            ("/Users/example/src/sample-suite/sample-stack/docker-compose.yml",),
            backend.compose_config_files,
        )

        web_client = services[1]
        self.assertEqual("web-client_PROJ-123", web_client.primary_repo_name)
        self.assertEqual("PROJ-123", web_client.primary_branch)
        self.assertTrue(web_client.primary_is_worktree)

        worker = services[2]
        self.assertEqual("worker-service", worker.primary_repo_name)
        self.assertEqual(1, worker.extra_git_mounts)

    def test_gather_services_returns_error_without_snapshot_on_docker_failure(
        self,
    ) -> None:
        with patch.object(
            dcmon,
            "list_compose_container_ids",
            side_effect=dcmon.DcmonProbeError("docker failed"),
        ):
            services, error = dcmon.gather_services()
        self.assertIsNone(services)
        self.assertEqual("docker failed", error)

    def test_render_detail_text_includes_worktree_and_error(self) -> None:
        mount = dcmon.MountInfo(
            kind="bind",
            source="/Users/example/src/sample-suite/web-client_PROJ-123",
            target="/app",
            git=dcmon.GitProbeInfo(
                repo_root="/Users/example/src/sample-suite/web-client_PROJ-123",
                repo_name="web-client_PROJ-123",
                branch="PROJ-123",
                is_worktree=True,
                main_repo_path="/Users/example/src/sample-suite/web-client",
                common_dir="/Users/example/src/sample-suite/web-client/.git",
            ),
        )
        service = dcmon.ServiceInfo(
            project_name="sample-stack",
            service_name="web-client",
            compose_workdir="/Users/example/src/sample-suite/sample-stack",
            containers=[
                dcmon.ContainerInfo(
                    container_id="web-client-id",
                    name="web-client",
                    service="web-client",
                    state="running",
                    mounts=[mount],
                )
            ],
            primary_repo_name="web-client_PROJ-123",
            primary_branch="PROJ-123",
            primary_is_worktree=True,
        )

        detail = dcmon.render_detail_text(service, "docker is slow")
        self.assertIn("error: docker is slow", detail)
        expected_main_checkout = dcmon.format_path(
            "/Users/example/src/sample-suite/web-client"
        )
        self.assertIn(f"main checkout: {expected_main_checkout}", detail)
        self.assertIn("[web-client_PROJ-123@PROJ-123 W]", detail)

    def test_service_sort_key_orders_worktree_then_base_then_other_then_unknown(
        self,
    ) -> None:
        worktree = dcmon.ServiceInfo(
            project_name="sample-stack",
            service_name="web-client",
            compose_workdir="/tmp",
            primary_repo_name="web-client_PROJ-123",
            primary_branch="PROJ-123",
            primary_is_worktree=True,
        )
        base = dcmon.ServiceInfo(
            project_name="sample-stack",
            service_name="backend",
            compose_workdir="/tmp",
            primary_repo_name="api-service",
            primary_branch="master",
            primary_is_worktree=False,
        )
        other = dcmon.ServiceInfo(
            project_name="sample-stack",
            service_name="feature-service",
            compose_workdir="/tmp",
            primary_repo_name="feature-repo",
            primary_branch="feature/foo",
            primary_is_worktree=False,
        )
        unknown = dcmon.ServiceInfo(
            project_name="sample-stack",
            service_name="unknown-service",
            compose_workdir="/tmp",
        )

        ordered = sorted([unknown, other, base, worktree], key=dcmon.service_sort_key)
        self.assertEqual(
            ["web-client", "backend", "feature-service", "unknown-service"],
            [service.service_name for service in ordered],
        )


class SwitchPlanTests(unittest.TestCase):
    def test_build_switch_plan_matches_ticket_and_falls_back_to_primary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            web_client_base = tmp_path / "web-client"
            web_client_match = tmp_path / "web-client_PROJ-123"
            backend_base = tmp_path / "api-service"
            backend_feature = tmp_path / "api-service_feature"
            for path in [
                web_client_base,
                web_client_match,
                backend_base,
                backend_feature,
            ]:
                path.mkdir()

            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(web_client_base),
                        repo_name="web-client",
                        branch="master",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(web_client_match),
                        repo_name="web-client_PROJ-123",
                        branch="PROJ-123",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=True,
                        token="PROJ-123",
                        branch_token="PROJ-123",
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(backend_base),
                        repo_name="api-service",
                        branch="master",
                        common_dir=str(tmp_path / "api-service.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(backend_feature),
                        repo_name="api-service_feature",
                        branch="PROJ-126",
                        common_dir=str(tmp_path / "api-service.git"),
                        is_worktree=True,
                        token="PROJ-126",
                        branch_token="PROJ-126",
                    ),
                ],
                tmp_path,
            )

            web_client_service = make_service(
                service_name="web-client",
                compose_file=str(compose_file),
                mount_source=str(web_client_base),
                branch="master",
                common_dir=str(tmp_path / "web-client.git"),
            )
            backend_service = make_service(
                service_name="backend",
                compose_file=str(compose_file),
                mount_source=str(backend_feature),
                branch="PROJ-126",
                common_dir=str(tmp_path / "api-service.git"),
                main_repo_path=str(backend_base),
            )

            plan = dcmon.build_switch_plan(
                "PROJ-123", [web_client_service, backend_service], workspace
            )

        self.assertEqual("PROJ-123", plan.ticket_token)
        self.assertEqual(2, len(plan.service_plans))
        self.assertEqual(2, plan.changed_mount_count())
        self.assertEqual(1, plan.fallback_mount_count())

        by_service = {
            service_plan.service_name: service_plan
            for service_plan in plan.service_plans
        }
        self.assertEqual(
            str(web_client_match),
            by_service["web-client"].changed_git_mounts()[0].planned_source,
        )
        self.assertEqual(
            "ticket", by_service["web-client"].changed_git_mounts()[0].note
        )
        self.assertEqual(
            str(backend_base),
            by_service["backend"].changed_git_mounts()[0].planned_source,
        )
        self.assertEqual("base", by_service["backend"].changed_git_mounts()[0].note)

    def test_build_switch_plan_preserves_relative_subpath(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            repo_base = tmp_path / "api-service"
            repo_match = tmp_path / "api-service_PROJ-123"
            relative_file = Path("config") / "dev.env"
            (repo_base / relative_file.parent).mkdir(parents=True)
            (repo_match / relative_file.parent).mkdir(parents=True)
            (repo_base / relative_file).write_text("base\n")
            (repo_match / relative_file).write_text("match\n")

            service = make_service(
                service_name="backend",
                compose_file=str(compose_file),
                mount_source=str(repo_base / relative_file),
                repo_root=str(repo_base),
                branch="master",
                common_dir=str(tmp_path / "api-service.git"),
            )
            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_base),
                        repo_name="api-service",
                        branch="master",
                        common_dir=str(tmp_path / "api-service.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_match),
                        repo_name="api-service_PROJ-123",
                        branch="PROJ-123",
                        common_dir=str(tmp_path / "api-service.git"),
                        is_worktree=True,
                        token="PROJ-123",
                        branch_token="PROJ-123",
                    ),
                ],
                tmp_path,
            )

            plan = dcmon.build_switch_plan("PROJ-123", [service], workspace)

        changed_mount = plan.service_plans[0].changed_git_mounts()[0]
        self.assertEqual(str(repo_match / relative_file), changed_mount.planned_source)

    def test_build_switch_plan_recovers_stale_worktree_source_by_family_name(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            repo_base = tmp_path / "api-service"
            repo_match = tmp_path / "api-service_PROJ-125"
            stale_source = tmp_path / "api-service_PROJ-123"
            repo_base.mkdir()
            repo_match.mkdir()

            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_base),
                        repo_name="api-service",
                        branch="master",
                        common_dir=str(tmp_path / "api-service.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_match),
                        repo_name="api-service_PROJ-125",
                        branch="PROJ-125",
                        common_dir=str(tmp_path / "api-service.git"),
                        is_worktree=True,
                        token="PROJ-125",
                        branch_token="PROJ-125",
                    ),
                ],
                tmp_path,
            )

            service = dcmon.ServiceInfo(
                project_name="sample-stack",
                service_name="backend",
                compose_workdir=str(tmp_path),
                compose_config_files=(str(compose_file),),
                containers=[
                    dcmon.ContainerInfo(
                        container_id="backend-id",
                        name="backend",
                        service="backend",
                        state="running",
                        mounts=[
                            dcmon.MountInfo(
                                kind="bind", source=str(stale_source), target="/app"
                            )
                        ],
                    )
                ],
            )

            plan = dcmon.build_switch_plan("PROJ-125", [service], workspace)

        self.assertEqual(1, len(plan.service_plans))
        changed_mount = plan.service_plans[0].changed_git_mounts()[0]
        self.assertEqual(str(repo_match), changed_mount.planned_source)
        self.assertEqual("ticket", changed_mount.note)

    def test_build_ticket_options_count_stale_worktree_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            repo_base = tmp_path / "api-service"
            repo_match = tmp_path / "api-service_PROJ-125"
            stale_source = tmp_path / "api-service_PROJ-123"
            repo_base.mkdir()
            repo_match.mkdir()

            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_base),
                        repo_name="api-service",
                        branch="master",
                        common_dir=str(tmp_path / "api-service.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_match),
                        repo_name="api-service_PROJ-125",
                        branch="PROJ-125",
                        common_dir=str(tmp_path / "api-service.git"),
                        is_worktree=True,
                        token="PROJ-125",
                        branch_token="PROJ-125",
                    ),
                ],
                tmp_path,
            )

            service = dcmon.ServiceInfo(
                project_name="sample-stack",
                service_name="backend",
                compose_workdir=str(tmp_path),
                compose_config_files=(str(compose_file),),
                containers=[
                    dcmon.ContainerInfo(
                        container_id="backend-id",
                        name="backend",
                        service="backend",
                        state="running",
                        mounts=[
                            dcmon.MountInfo(
                                kind="bind", source=str(stale_source), target="/app"
                            )
                        ],
                    )
                ],
            )

            options = dcmon.build_ticket_options([service], workspace)

        self.assertEqual(["PROJ-125"], [option.token for option in options])
        self.assertEqual([1], [option.match_count for option in options])

    def test_build_ticket_options_sorts_by_changed_match_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            repo_a = tmp_path / "repo-a"
            repo_a_991 = tmp_path / "repo-a_PROJ-123"
            repo_b = tmp_path / "repo-b"
            repo_b_873 = tmp_path / "repo-b_PROJ-126"
            for path in [repo_a, repo_a_991, repo_b, repo_b_873]:
                path.mkdir()

            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_a),
                        repo_name="repo-a",
                        branch="main",
                        common_dir=str(tmp_path / "repo-a.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_a_991),
                        repo_name="repo-a_PROJ-123",
                        branch="PROJ-123",
                        common_dir=str(tmp_path / "repo-a.git"),
                        is_worktree=True,
                        token="PROJ-123",
                        branch_token="PROJ-123",
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_b),
                        repo_name="repo-b",
                        branch="main",
                        common_dir=str(tmp_path / "repo-b.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_b_873),
                        repo_name="repo-b_PROJ-126",
                        branch="PROJ-126",
                        common_dir=str(tmp_path / "repo-b.git"),
                        is_worktree=True,
                        token="PROJ-126",
                        branch_token="PROJ-126",
                    ),
                ],
                tmp_path,
            )

            services = [
                make_service(
                    service_name="service-a",
                    compose_file=str(compose_file),
                    mount_source=str(repo_a),
                    branch="main",
                    common_dir=str(tmp_path / "repo-a.git"),
                ),
                make_service(
                    service_name="service-b",
                    compose_file=str(compose_file),
                    mount_source=str(repo_b),
                    branch="main",
                    common_dir=str(tmp_path / "repo-b.git"),
                ),
            ]

            options = dcmon.build_ticket_options(services, workspace)

        self.assertEqual(["PROJ-123", "PROJ-126"], [option.token for option in options])
        self.assertEqual([1, 1], [option.match_count for option in options])
        self.assertEqual(
            [("repo-a",), ("repo-b",)], [option.repo_names for option in options]
        )

    def test_build_ticket_options_include_already_active_ticket_repo(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            repo_base = tmp_path / "web-client"
            repo_match = tmp_path / "web-client_PROJ-123"
            repo_base.mkdir()
            repo_match.mkdir()

            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_base),
                        repo_name="web-client",
                        branch="master",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_match),
                        repo_name="web-client_PROJ-123",
                        branch="PROJ-123",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=True,
                        token="PROJ-123",
                        branch_token="PROJ-123",
                    ),
                ],
                tmp_path,
            )

            services = [
                make_service(
                    service_name="web-client",
                    compose_file=str(compose_file),
                    mount_source=str(repo_match),
                    branch="PROJ-123",
                    common_dir=str(tmp_path / "web-client.git"),
                    main_repo_path=str(repo_base),
                )
            ]

            options = dcmon.build_ticket_options(services, workspace)

        self.assertEqual(["PROJ-123"], [option.token for option in options])
        self.assertEqual([1], [option.match_count for option in options])
        self.assertEqual([("web-client",)], [option.repo_names for option in options])

    def test_build_ticket_options_show_discovered_repo_without_active_match(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            repo_base = tmp_path / "web-client"
            repo_active = tmp_path / "web-client_PROJ-123"
            repo_other = tmp_path / "web-client_PROJ-124"
            repo_base.mkdir()
            repo_active.mkdir()
            repo_other.mkdir()

            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_base),
                        repo_name="web-client",
                        branch="master",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_active),
                        repo_name="web-client_PROJ-123",
                        branch="PROJ-123",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=True,
                        token="PROJ-123",
                        branch_token="PROJ-123",
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_other),
                        repo_name="web-client_PROJ-124",
                        branch="PROJ-124",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=True,
                        token="PROJ-124",
                        branch_token="PROJ-124",
                    ),
                ],
                tmp_path,
            )

            services = [
                make_service(
                    service_name="web-client",
                    compose_file=str(compose_file),
                    mount_source=str(repo_active),
                    branch="PROJ-123",
                    common_dir=str(tmp_path / "web-client.git"),
                    main_repo_path=str(repo_base),
                )
            ]

            options = dcmon.build_ticket_options(services, workspace)

        by_token = {option.token: option for option in options}
        self.assertEqual(1, by_token["PROJ-123"].match_count)
        self.assertEqual(("web-client",), by_token["PROJ-123"].repo_names)
        self.assertEqual(1, by_token["PROJ-124"].match_count)
        self.assertEqual(("web-client",), by_token["PROJ-124"].repo_names)

    def test_no_change_status_for_ticket_reports_missing_active_services(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            web_client_base = tmp_path / "web-client"
            web_client_982 = tmp_path / "web-client_PROJ-124"
            backend_base = tmp_path / "backend"
            web_client_base.mkdir()
            web_client_982.mkdir()
            backend_base.mkdir()

            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(web_client_base),
                        repo_name="web-client",
                        branch="master",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(web_client_982),
                        repo_name="web-client_PROJ-124",
                        branch="PROJ-124",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=True,
                        token="PROJ-124",
                        branch_token="PROJ-124",
                    ),
                ],
                tmp_path,
            )

            services = [
                make_service(
                    service_name="backend",
                    compose_file=str(compose_file),
                    mount_source=str(backend_base),
                    branch="master",
                    common_dir=str(tmp_path / "backend.git"),
                )
            ]

            status = dcmon.no_change_status_for_ticket("PROJ-124", services, workspace)

        self.assertEqual(
            "PROJ-124: no active services for web-client; no changes done", status
        )

    def test_no_change_status_for_ticket_reports_already_active_ticket(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")

            web_client_base = tmp_path / "web-client"
            web_client_991 = tmp_path / "web-client_PROJ-123"
            web_client_base.mkdir()
            web_client_991.mkdir()

            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(web_client_base),
                        repo_name="web-client",
                        branch="master",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(web_client_991),
                        repo_name="web-client_PROJ-123",
                        branch="PROJ-123",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=True,
                        token="PROJ-123",
                        branch_token="PROJ-123",
                    ),
                ],
                tmp_path,
            )

            services = [
                make_service(
                    service_name="web-client",
                    compose_file=str(compose_file),
                    mount_source=str(web_client_991),
                    branch="PROJ-123",
                    common_dir=str(tmp_path / "web-client.git"),
                    main_repo_path=str(web_client_base),
                )
            ]

            status = dcmon.no_change_status_for_ticket("PROJ-123", services, workspace)

        self.assertEqual(
            "PROJ-123: already active for web-client; no changes done", status
        )

    def test_filter_ticket_options_matches_token_and_repo_names(self) -> None:
        options = [
            dcmon.TicketOption(
                token="PROJ-126", match_count=1, repo_names=("backend",)
            ),
            dcmon.TicketOption(
                token="PROJ-123", match_count=2, repo_names=("web-client", "filematch")
            ),
        ]

        self.assertEqual(
            ["PROJ-123"],
            [option.token for option in dcmon.filter_ticket_options(options, "123")],
        )
        self.assertEqual(
            ["PROJ-123"],
            [
                option.token
                for option in dcmon.filter_ticket_options(options, "filematch")
            ],
        )
        self.assertEqual(
            ["PROJ-126", "PROJ-123"],
            [option.token for option in dcmon.filter_ticket_options(options, "")],
        )

    def test_filter_preview_rows_matches_any_visible_column(self) -> None:
        rows = [
            (
                "sample-stack",
                "backend",
                "/app",
                "~/src/sample-suite/api-service",
                "~/src/sample-suite/api-service_PROJ-123",
                "ticket",
            ),
            (
                "sample-stack",
                "web-client",
                "/app",
                "~/src/sample-suite/web-client",
                "~/src/sample-suite/web-client",
                "base",
            ),
        ]

        filtered = dcmon.filter_preview_rows(rows, "api-service")
        self.assertEqual([rows[0]], filtered)
        filtered = dcmon.filter_preview_rows(rows, "base")
        self.assertEqual([rows[1]], filtered)

    def test_render_compose_override_preserves_volume_and_tmpfs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")
            repo_base = tmp_path / "worker-service"
            repo_match = tmp_path / "worker-service_PROJ-123"
            repo_base.mkdir()
            repo_match.mkdir()

            service = make_service(
                service_name="worker",
                compose_file=str(compose_file),
                mount_source=str(repo_base),
                branch="master",
                common_dir=str(tmp_path / "worker-service.git"),
                extra_mounts=[
                    dcmon.MountInfo(
                        kind="volume",
                        source="/var/lib/docker/volumes/sample-stack-worker-node-modules/_data",
                        target="/app/node_modules",
                        name="sample-stack-worker-node-modules",
                    ),
                    dcmon.MountInfo(kind="tmpfs", source=None, target="/tmp/cache"),
                ],
            )
            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_base),
                        repo_name="worker-service",
                        branch="master",
                        common_dir=str(tmp_path / "worker-service.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_match),
                        repo_name="worker-service_PROJ-123",
                        branch="PROJ-123",
                        common_dir=str(tmp_path / "worker-service.git"),
                        is_worktree=True,
                        token="PROJ-123",
                        branch_token="PROJ-123",
                    ),
                ],
                tmp_path,
            )
            plan = dcmon.build_switch_plan("PROJ-123", [service], workspace)
            yaml_text = dcmon.render_compose_override(
                plan.executable_service_plans(),
                {
                    "worker": {
                        "/app/node_modules": "worker-node-modules:/app/node_modules"
                    }
                },
            )

        self.assertIn(f"{repo_match}:/app", yaml_text)
        self.assertIn("worker-node-modules:/app/node_modules", yaml_text)
        self.assertIn('"/tmp/cache"', yaml_text)

    def test_preview_rows_include_skipped_service(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            missing_plan = dcmon.ServiceSwitchPlan(
                ticket_token="PROJ-123",
                service=dcmon.ServiceInfo(
                    project_name="sample-stack",
                    service_name="backend",
                    compose_workdir=str(tmp_path),
                ),
                durable_compose_files=(),
                skipped_reason="no durable compose config files found",
            )
            plan = dcmon.SwitchPlan(
                ticket_token="PROJ-123", service_plans=[missing_plan]
            )

        rows = dcmon.preview_rows_for_plan(plan)
        self.assertEqual(
            [
                (
                    "sample-stack",
                    "backend",
                    "-",
                    "-",
                    "-",
                    "skip: no durable compose config files found",
                )
            ],
            rows,
        )

    def test_execute_switch_plan_recreates_services_with_fresh_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            compose_file = tmp_path / "docker-compose.yml"
            compose_file.write_text("services:\n")
            repo_base = tmp_path / "web-client"
            repo_match = tmp_path / "web-client_PROJ-123"
            repo_base.mkdir()
            repo_match.mkdir()

            service = make_service(
                service_name="web-client",
                compose_file=str(compose_file),
                mount_source=str(repo_base),
                branch="master",
                common_dir=str(tmp_path / "web-client.git"),
            )
            workspace = dcmon.build_workspace_index(
                [
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_base),
                        repo_name="web-client",
                        branch="master",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=False,
                        token=None,
                        branch_token=None,
                    ),
                    dcmon.WorkspaceCheckout(
                        checkout_path=str(repo_match),
                        repo_name="web-client_PROJ-123",
                        branch="PROJ-123",
                        common_dir=str(tmp_path / "web-client.git"),
                        is_worktree=True,
                        token="PROJ-123",
                        branch_token="PROJ-123",
                    ),
                ],
                tmp_path,
            )
            plan = dcmon.build_switch_plan("PROJ-123", [service], workspace)

            refreshed_service = make_service(
                service_name="web-client",
                compose_file=str(compose_file),
                mount_source=str(repo_match),
                branch="PROJ-123",
                common_dir=str(tmp_path / "web-client.git"),
                main_repo_path=str(repo_base),
            )

            recorded: list[tuple[list[str], Optional[str]]] = []

            def fake_run(
                cmd: list[str],
                timeout: float = dcmon.DEFAULT_TIMEOUT,
                cwd: Optional[str] = None,
            ) -> str:
                recorded.append((cmd, cwd))
                if "config" in cmd:
                    return json.dumps(
                        {
                            "services": {
                                "web-client": {
                                    "volumes": [
                                        {
                                            "type": "bind",
                                            "source": str(repo_base),
                                            "target": "/app",
                                        },
                                        {
                                            "type": "volume",
                                            "source": "web-client-modules",
                                            "target": "/app/node_modules",
                                        },
                                    ]
                                }
                            }
                        }
                    )
                return ""

            with patch.object(dcmon, "_run", side_effect=fake_run):
                with patch.object(
                    dcmon, "gather_services", return_value=([refreshed_service], None)
                ):
                    result = dcmon.execute_switch_plan(plan)

        self.assertEqual(["sample-stack/web-client"], result.applied_services)
        self.assertEqual([], result.apply_errors)
        config_cmd, config_cwd = recorded[0]
        self.assertEqual(
            [
                "docker",
                "compose",
                "--project-name",
                "sample-stack",
                "-f",
                str(compose_file),
                "config",
                "--format",
                "json",
            ],
            config_cmd,
        )
        self.assertEqual(str(tmp_path), config_cwd)
        compose_cmd, compose_cwd = recorded[1]
        self.assertIn(str(compose_file), compose_cmd)
        first_f_index = compose_cmd.index("-f")
        second_f_index = compose_cmd.index("-f", first_f_index + 1)
        override_path = compose_cmd[second_f_index + 1]
        self.assertTrue(
            override_path.startswith(f"/tmp/{dcmon.WORKTREE_OVERRIDE_PREFIX}.")
        )
        self.assertFalse(Path(override_path).exists())
        self.assertEqual(str(tmp_path), compose_cwd)
        self.assertEqual([], result.verification_errors)


if __name__ == "__main__":
    unittest.main()
