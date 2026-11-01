"""Build provenance checks use temporary Git repositories and fake Go/Docker tools."""
import os
from pathlib import Path
import shutil
import runpy
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).with_name("build.sh")
require_clean_probe = runpy.run_path(str(Path(__file__).with_name("build_shutdown_probe.py")))["require_clean_source"]


@unittest.skipUnless(shutil.which("bash") and shutil.which("git"), "requires bash and git")
class BuildTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "application"
        self.storage = self.root / "storage"
        self.repo.mkdir()
        self.storage.mkdir()
        self.tools = self.root / "tools"
        self.tools.mkdir()
        self.log = self.root / "commands.log"
        self.env = dict(os.environ, PATH=str(self.tools) + os.pathsep + os.environ["PATH"], BUILD_TEST_LOG=str(self.log),
                        GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        self.env.pop("BENCH_STORAGE_DIR", None)
        self.env.pop("OTELDB_BENCH_IMAGE", None)
        for name, script in {
            "go": '#!/bin/sh\nprintf "go %s\\n" "$*" >> "$BUILD_TEST_LOG"\nif [ "$1" = list ]; then echo v0.0.0-test; fi\n',
            "docker": '#!/bin/sh\nprintf "docker %s\\n" "$*" >> "$BUILD_TEST_LOG"\n',
        }.items():
            path = self.tools / name
            path.write_text(script)
            path.chmod(0o755)
        for repo in [self.repo, self.storage]:
            self.git(repo, "init", "-q")
            (repo / "go.mod").write_text("module example.test/fixture\n")
            (repo / "go.sum").write_text("")
            (repo / "main.go").write_text("package main\n")
        bench = self.repo / "dev/local/scale-bench"
        bench.mkdir(parents=True)
        shutil.copyfile(SOURCE, bench / "build.sh")
        (bench / ".gitignore").write_text("/bin/\n/results/\n")
        for repo in [self.repo, self.storage]:
            self.git(repo, "add", "go.mod", "go.sum", "main.go")
        self.git(self.repo, "add", "dev/local/scale-bench/build.sh", "dev/local/scale-bench/.gitignore")
        for repo in [self.repo, self.storage]:
            self.git(repo, "-c", "user.name=Benchmark Test", "-c", "user.email=benchmark@example.test", "-c",
                     "commit.gpgsign=false", "commit", "-qm", "fixture")

    def git(self, repo, *args):
        return subprocess.run(["git", "-C", str(repo), *args], env=self.env, check=True, text=True, capture_output=True)

    def build(self, storage=False):
        env = dict(self.env)
        if storage:
            env["BENCH_STORAGE_DIR"] = str(self.storage)
        return subprocess.run(["bash", "dev/local/scale-bench/build.sh"], cwd=self.repo, env=env, text=True, capture_output=True)

    def test_clean_sources_allow_ignored_outputs(self):
        for relative in ["dev/local/scale-bench/results/old/profile.pb.gz", "dev/local/scale-bench/bin/old-binary", ".worktrees/other/ignored.go"]:
            path = self.repo / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("existing artifact")
        for storage in [False, True]:
            with self.subTest(storage=storage):
                result = self.build(storage)
                self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("docker build", self.log.read_text())
        self.assertEqual(self.git(self.repo, "diff", "--", "go.mod", "go.sum").stdout, "")

    def test_dirty_application_is_rejected_before_build(self):
        for name in ["main.go", "new-source.go"]:
            with self.subTest(name=name):
                path = self.repo / name
                original = path.read_text() if path.exists() else None
                path.write_text("package changed\n")
                result = self.build()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("commit source changes", result.stderr)
                self.assertFalse(self.log.exists())
                if original is None:
                    path.unlink()
                else:
                    path.write_text(original)

    def test_untracked_storage_source_is_rejected_before_build(self):
        (self.storage / "new-source.go").write_text("package newsource\n")
        result = self.build(True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(self.storage), result.stderr)
        self.assertIn("new-source.go", result.stderr)
        self.assertFalse(self.log.exists())

    def test_shutdown_probe_rejects_uncommitted_source(self):
        require_clean_probe(self.repo)
        (self.repo / "new-source.go").write_text("package prototype\n")
        with self.assertRaisesRegex(SystemExit, "commit source changes"):
            require_clean_probe(self.repo)


if __name__ == "__main__":
    unittest.main()
