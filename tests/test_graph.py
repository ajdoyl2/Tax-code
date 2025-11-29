"""
Tests for Neo4j graph ingestion module.

These tests verify the data preparation logic without requiring
a running Neo4j instance.
"""

import unittest
from unittest.mock import MagicMock, patch

from src.models import LegalNode, NodeStatus, NodeType, Reference, ParsedTaxCode
from src.graph import (
    flatten_nodes,
    extract_parent_relationships,
    extract_all_references,
    Neo4jConfig,
)


class TestFlattenNodes(unittest.TestCase):
    """Test the flatten_nodes utility function."""

    def test_flatten_single_node(self):
        """Test flattening a single node with no children."""
        node = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
        )
        result = flatten_nodes(node)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "26 USC 1")

    def test_flatten_nested_nodes(self):
        """Test flattening a nested hierarchy."""
        child1 = LegalNode(
            id="26 USC 1(a)",
            identifier="/us/usc/t26/s1/a",
            node_type=NodeType.SUBSECTION,
            heading="Subsection A",
        )
        child2 = LegalNode(
            id="26 USC 1(b)",
            identifier="/us/usc/t26/s1/b",
            node_type=NodeType.SUBSECTION,
            heading="Subsection B",
        )
        parent = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
            children=[child1, child2],
        )

        result = flatten_nodes(parent)
        self.assertEqual(len(result), 3)
        ids = [n.id for n in result]
        self.assertIn("26 USC 1", ids)
        self.assertIn("26 USC 1(a)", ids)
        self.assertIn("26 USC 1(b)", ids)

    def test_flatten_deep_hierarchy(self):
        """Test flattening a 3-level hierarchy."""
        grandchild = LegalNode(
            id="26 USC 1(a)(1)",
            identifier="/us/usc/t26/s1/a/1",
            node_type=NodeType.PARAGRAPH,
            heading="Paragraph 1",
        )
        child = LegalNode(
            id="26 USC 1(a)",
            identifier="/us/usc/t26/s1/a",
            node_type=NodeType.SUBSECTION,
            heading="Subsection A",
            children=[grandchild],
        )
        parent = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
            children=[child],
        )

        result = flatten_nodes(parent)
        self.assertEqual(len(result), 3)


class TestExtractParentRelationships(unittest.TestCase):
    """Test the extract_parent_relationships utility function."""

    def test_no_children(self):
        """Test node with no children."""
        node = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
        )
        result = extract_parent_relationships(node)
        self.assertEqual(len(result), 0)

    def test_single_child(self):
        """Test node with one child."""
        child = LegalNode(
            id="26 USC 1(a)",
            identifier="/us/usc/t26/s1/a",
            node_type=NodeType.SUBSECTION,
            heading="Subsection A",
        )
        parent = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
            children=[child],
        )

        result = extract_parent_relationships(parent)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], ("26 USC 1", "26 USC 1(a)"))

    def test_nested_relationships(self):
        """Test extracting relationships from nested hierarchy."""
        grandchild = LegalNode(
            id="26 USC 1(a)(1)",
            identifier="/us/usc/t26/s1/a/1",
            node_type=NodeType.PARAGRAPH,
            heading="Paragraph 1",
        )
        child = LegalNode(
            id="26 USC 1(a)",
            identifier="/us/usc/t26/s1/a",
            node_type=NodeType.SUBSECTION,
            heading="Subsection A",
            children=[grandchild],
        )
        parent = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
            children=[child],
        )

        result = extract_parent_relationships(parent)
        self.assertEqual(len(result), 2)
        self.assertIn(("26 USC 1", "26 USC 1(a)"), result)
        self.assertIn(("26 USC 1(a)", "26 USC 1(a)(1)"), result)


class TestExtractAllReferences(unittest.TestCase):
    """Test the extract_all_references utility function."""

    def test_no_references(self):
        """Test node with no references."""
        node = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
        )
        result = extract_all_references(node)
        self.assertEqual(len(result), 0)

    def test_with_references(self):
        """Test node with references."""
        ref = Reference(
            target_section="162",
            context="as defined in section 162",
            reference_type="definition",
        )
        node = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
            references=[ref],
        )

        result = extract_all_references(node)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], "26 USC 1")
        self.assertEqual(result[0][1].target_section, "162")

    def test_nested_references(self):
        """Test extracting references from nested nodes."""
        ref1 = Reference(
            target_section="162",
            context="section 162",
            reference_type="general",
        )
        ref2 = Reference(
            target_section="274",
            context="section 274",
            reference_type="exception",
        )
        child = LegalNode(
            id="26 USC 1(a)",
            identifier="/us/usc/t26/s1/a",
            node_type=NodeType.SUBSECTION,
            heading="Subsection A",
            references=[ref2],
        )
        parent = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            heading="Test",
            references=[ref1],
            children=[child],
        )

        result = extract_all_references(parent)
        self.assertEqual(len(result), 2)
        targets = [r[1].target_section for r in result]
        self.assertIn("162", targets)
        self.assertIn("274", targets)


class TestNeo4jConfig(unittest.TestCase):
    """Test Neo4j configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = Neo4jConfig()
        self.assertEqual(config.uri, "bolt://localhost:7687")
        self.assertEqual(config.username, "neo4j")
        self.assertEqual(config.database, "neo4j")

    @patch.dict("os.environ", {
        "NEO4J_URI": "bolt://custom:7687",
        "NEO4J_USERNAME": "admin",
        "NEO4J_PASSWORD": "secret",
        "NEO4J_DATABASE": "taxcode",
    })
    def test_config_from_env(self):
        """Test configuration from environment variables."""
        config = Neo4jConfig.from_env()
        self.assertEqual(config.uri, "bolt://custom:7687")
        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.database, "taxcode")


class TestDataPreparation(unittest.TestCase):
    """Test data preparation for Neo4j ingestion."""

    def setUp(self):
        """Set up test data."""
        # Create a simple hierarchy
        self.subsection = LegalNode(
            id="26 USC 1(a)",
            identifier="/us/usc/t26/s1/a",
            node_type=NodeType.SUBSECTION,
            num="(a)",
            heading="General rule",
            text="This is the general rule referencing section 162.",
            references=[
                Reference(
                    target_section="162",
                    context="referencing section 162",
                    reference_type="general",
                )
            ],
        )
        self.section = LegalNode(
            id="26 USC 1",
            identifier="/us/usc/t26/s1",
            node_type=NodeType.SECTION,
            num="1",
            heading="Tax imposed",
            text="Tax is imposed.",
            children=[self.subsection],
        )
        self.chapter = LegalNode(
            id="26 USC Chapter 1",
            identifier="/us/usc/t26/ch1",
            node_type=NodeType.CHAPTER,
            num="1",
            heading="Normal Taxes",
            children=[self.section],
        )

    def test_full_data_extraction(self):
        """Test extracting all data needed for Neo4j."""
        # Flatten nodes
        nodes = flatten_nodes(self.chapter)
        self.assertEqual(len(nodes), 3)

        # Extract parent relationships
        parent_rels = extract_parent_relationships(self.chapter)
        self.assertEqual(len(parent_rels), 2)

        # Extract references
        refs = extract_all_references(self.chapter)
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0][0], "26 USC 1(a)")
        self.assertEqual(refs[0][1].target_section, "162")

    def test_node_properties(self):
        """Test that nodes have all required properties."""
        nodes = flatten_nodes(self.chapter)

        for node in nodes:
            # All nodes should have these properties
            self.assertIsNotNone(node.id)
            self.assertIsNotNone(node.identifier)
            self.assertIsNotNone(node.node_type)
            self.assertIsNotNone(node.status)

            # Check computed properties
            self.assertIsInstance(node.is_container, bool)
            self.assertIsInstance(node.is_content, bool)


if __name__ == "__main__":
    unittest.main()
