"""
Tests for Archon MCP Server installation and connectivity.

These tests verify:
1. Configuration files exist and are valid
2. Docker services are running (when started)
3. MCP endpoints are accessible
4. Basic RAG functionality works
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional
from unittest import TestCase, main, skipIf

# Try to import httpx for HTTP tests
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


class TestArchonConfiguration(TestCase):
    """Test Archon configuration files."""

    def setUp(self):
        self.project_root = Path(__file__).parent.parent
        self.archon_dir = self.project_root / "archon"

    def test_mcp_json_exists(self):
        """Verify .mcp.json configuration file exists."""
        mcp_json = self.project_root / ".mcp.json"
        self.assertTrue(mcp_json.exists(), ".mcp.json file should exist")

    def test_mcp_json_valid(self):
        """Verify .mcp.json is valid JSON with correct structure."""
        mcp_json = self.project_root / ".mcp.json"
        with open(mcp_json) as f:
            config = json.load(f)

        self.assertIn("mcpServers", config, "Should have mcpServers key")
        self.assertIn("archon", config["mcpServers"], "Should have archon server config")

        archon_config = config["mcpServers"]["archon"]
        self.assertIn("url", archon_config, "Should have url field")
        self.assertIn("8051", archon_config["url"], "Should use port 8051")

    def test_archon_submodule_exists(self):
        """Verify Archon submodule is cloned."""
        self.assertTrue(self.archon_dir.exists(), "archon/ directory should exist")
        self.assertTrue(
            (self.archon_dir / "docker-compose.yml").exists(),
            "docker-compose.yml should exist in archon/"
        )

    def test_gitmodules_configured(self):
        """Verify .gitmodules has Archon configured."""
        gitmodules = self.project_root / ".gitmodules"
        self.assertTrue(gitmodules.exists(), ".gitmodules should exist")

        content = gitmodules.read_text()
        self.assertIn("archon", content, "Should reference archon submodule")
        self.assertIn("coleam00/archon", content, "Should point to correct repo")

    def test_env_example_exists(self):
        """Verify .env.example template exists."""
        env_example = self.archon_dir / ".env.example"
        self.assertTrue(env_example.exists(), ".env.example should exist")

    def test_env_file_exists(self):
        """Verify .env file was created from example."""
        env_file = self.archon_dir / ".env"
        self.assertTrue(
            env_file.exists(),
            ".env file should exist. Run: cp archon/.env.example archon/.env"
        )

    def test_migration_files_exist(self):
        """Verify database migration files exist."""
        migration_dir = self.archon_dir / "migration"
        self.assertTrue(migration_dir.exists(), "migration/ directory should exist")

        complete_setup = migration_dir / "complete_setup.sql"
        self.assertTrue(
            complete_setup.exists(),
            "complete_setup.sql should exist for database setup"
        )


class TestDockerServices(TestCase):
    """Test Docker service status."""

    def setUp(self):
        self.project_root = Path(__file__).parent.parent
        self.archon_dir = self.project_root / "archon"

    def _docker_available(self) -> bool:
        """Check if Docker is available."""
        try:
            result = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except (subprocess.SubprocessError, FileNotFoundError):
            return False

    def _get_running_containers(self) -> list:
        """Get list of running Archon containers."""
        try:
            result = subprocess.run(
                ["docker", "compose", "ps", "--format", "json"],
                capture_output=True,
                text=True,
                cwd=self.archon_dir,
                timeout=30
            )
            if result.returncode == 0 and result.stdout.strip():
                # Docker compose outputs one JSON object per line
                containers = []
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        containers.append(json.loads(line))
                return containers
            return []
        except (subprocess.SubprocessError, json.JSONDecodeError):
            return []

    @skipIf(not os.environ.get("TEST_DOCKER"), "Set TEST_DOCKER=1 to run Docker tests")
    def test_docker_available(self):
        """Verify Docker is installed and running."""
        self.assertTrue(self._docker_available(), "Docker should be available")

    @skipIf(not os.environ.get("TEST_DOCKER"), "Set TEST_DOCKER=1 to run Docker tests")
    def test_archon_services_running(self):
        """Verify Archon Docker services are running."""
        if not self._docker_available():
            self.skipTest("Docker not available")

        containers = self._get_running_containers()

        # Check for expected services
        expected_services = ["archon-mcp", "archon-server", "archon-ui"]
        running_names = [c.get("Name", "") for c in containers]

        for service in expected_services:
            found = any(service in name for name in running_names)
            self.assertTrue(found, f"Service {service} should be running")


@skipIf(not HTTPX_AVAILABLE, "httpx not installed")
class TestMCPEndpoints(TestCase):
    """Test MCP server endpoints."""

    MCP_BASE_URL = "http://localhost:8051"

    def _is_server_running(self) -> bool:
        """Check if MCP server is reachable."""
        try:
            with httpx.Client(timeout=5) as client:
                response = client.get(f"{self.MCP_BASE_URL}/health")
                return response.status_code == 200
        except httpx.RequestError:
            return False

    @skipIf(not os.environ.get("TEST_MCP_LIVE"), "Set TEST_MCP_LIVE=1 to run live MCP tests")
    def test_mcp_server_health(self):
        """Verify MCP server health endpoint responds."""
        if not self._is_server_running():
            self.skipTest("MCP server not running")

        with httpx.Client(timeout=10) as client:
            response = client.get(f"{self.MCP_BASE_URL}/health")
            self.assertEqual(response.status_code, 200)

    @skipIf(not os.environ.get("TEST_MCP_LIVE"), "Set TEST_MCP_LIVE=1 to run live MCP tests")
    def test_sse_endpoint_accessible(self):
        """Verify SSE endpoint is accessible."""
        if not self._is_server_running():
            self.skipTest("MCP server not running")

        with httpx.Client(timeout=10) as client:
            # SSE endpoint should accept connections
            try:
                response = client.get(
                    f"{self.MCP_BASE_URL}/sse",
                    headers={"Accept": "text/event-stream"}
                )
                # Should get 200 or appropriate SSE response
                self.assertIn(response.status_code, [200, 400])
            except httpx.ReadTimeout:
                # SSE connections may timeout waiting for events - that's OK
                pass


class TestArchonIntegration(TestCase):
    """Integration tests for Archon RAG functionality."""

    SERVER_URL = "http://localhost:8181"

    def _is_server_running(self) -> bool:
        """Check if Archon server is reachable."""
        if not HTTPX_AVAILABLE:
            return False
        try:
            with httpx.Client(timeout=5) as client:
                response = client.get(f"{self.SERVER_URL}/health")
                return response.status_code == 200
        except httpx.RequestError:
            return False

    @skipIf(not HTTPX_AVAILABLE, "httpx not installed")
    @skipIf(not os.environ.get("TEST_MCP_LIVE"), "Set TEST_MCP_LIVE=1 to run live tests")
    def test_rag_sources_endpoint(self):
        """Verify RAG sources endpoint works."""
        if not self._is_server_running():
            self.skipTest("Archon server not running")

        with httpx.Client(timeout=10) as client:
            response = client.get(f"{self.SERVER_URL}/api/rag/sources")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIsInstance(data, (list, dict))


def run_tests_with_stop_on_failure() -> int:
    """
    Run tests and return exit code.
    Returns 0 on success, 1 on failure.
    """
    import unittest

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestArchonConfiguration))
    suite.addTests(loader.loadTestsFromTestCase(TestDockerServices))
    if HTTPX_AVAILABLE:
        suite.addTests(loader.loadTestsFromTestCase(TestMCPEndpoints))
        suite.addTests(loader.loadTestsFromTestCase(TestArchonIntegration))

    # Run with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(run_tests_with_stop_on_failure())
