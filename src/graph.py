"""
Neo4j Graph Database integration for US Tax Code.

This module handles:
- Connection to Neo4j database
- Creating TaxUnit nodes with properties
- Creating PARENT_OF relationships (hierarchy)
- Creating REFERENCES relationships (cross-references)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

from neo4j import GraphDatabase, Driver, Session
from neo4j.exceptions import ServiceUnavailable, AuthError

from .models import LegalNode, NodeStatus, NodeType, ParsedTaxCode, Reference

logger = logging.getLogger(__name__)


@dataclass
class Neo4jConfig:
    """Configuration for Neo4j connection."""
    uri: str = "bolt://localhost:7687"
    username: str = "neo4j"
    password: str = "password"
    database: str = "neo4j"

    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        """Create config from environment variables."""
        return cls(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            username=os.getenv("NEO4J_USERNAME", "neo4j"),
            password=os.getenv("NEO4J_PASSWORD", "password"),
            database=os.getenv("NEO4J_DATABASE", "neo4j"),
        )


class TaxCodeGraph:
    """
    Neo4j graph database manager for the Tax Code.

    Schema:
    - Node Label: TaxUnit
      - Properties: id, identifier, type, num, heading, text, status, path
    - Relationship: PARENT_OF
      - Direction: Parent -> Child
    - Relationship: REFERENCES
      - Properties: reference_type, context
      - Direction: Source -> Target
    """

    def __init__(self, config: Optional[Neo4jConfig] = None):
        """
        Initialize the graph manager.

        Args:
            config: Neo4j connection configuration. If None, uses env vars.
        """
        self.config = config or Neo4jConfig.from_env()
        self._driver: Optional[Driver] = None

    def connect(self) -> None:
        """Establish connection to Neo4j."""
        try:
            self._driver = GraphDatabase.driver(
                self.config.uri,
                auth=(self.config.username, self.config.password),
            )
            # Verify connectivity
            self._driver.verify_connectivity()
            logger.info(f"Connected to Neo4j at {self.config.uri}")
        except ServiceUnavailable as e:
            logger.error(f"Could not connect to Neo4j at {self.config.uri}: {e}")
            raise
        except AuthError as e:
            logger.error(f"Authentication failed for Neo4j: {e}")
            raise

    def close(self) -> None:
        """Close the Neo4j connection."""
        if self._driver:
            self._driver.close()
            self._driver = None
            logger.info("Disconnected from Neo4j")

    def __enter__(self) -> "TaxCodeGraph":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @property
    def driver(self) -> Driver:
        """Get the Neo4j driver, connecting if necessary."""
        if not self._driver:
            self.connect()
        return self._driver

    def create_indexes(self) -> None:
        """Create indexes for efficient querying."""
        with self.driver.session(database=self.config.database) as session:
            # Index on id for fast lookups
            session.run("""
                CREATE INDEX tax_unit_id IF NOT EXISTS
                FOR (n:TaxUnit) ON (n.id)
            """)

            # Index on type for filtering by node type
            session.run("""
                CREATE INDEX tax_unit_type IF NOT EXISTS
                FOR (n:TaxUnit) ON (n.type)
            """)

            # Index on identifier for XML path lookups
            session.run("""
                CREATE INDEX tax_unit_identifier IF NOT EXISTS
                FOR (n:TaxUnit) ON (n.identifier)
            """)

            # Index on status for filtering active/repealed
            session.run("""
                CREATE INDEX tax_unit_status IF NOT EXISTS
                FOR (n:TaxUnit) ON (n.status)
            """)

            # Full-text index for text search
            session.run("""
                CREATE FULLTEXT INDEX tax_unit_text IF NOT EXISTS
                FOR (n:TaxUnit) ON EACH [n.text, n.heading]
            """)

            logger.info("Created indexes on TaxUnit nodes")

    def clear_database(self) -> None:
        """Clear all TaxUnit nodes and relationships."""
        with self.driver.session(database=self.config.database) as session:
            result = session.run("""
                MATCH (n:TaxUnit)
                DETACH DELETE n
                RETURN count(n) as deleted
            """)
            deleted = result.single()["deleted"]
            logger.info(f"Cleared {deleted} TaxUnit nodes from database")

    def create_node(self, node: LegalNode) -> None:
        """
        Create a single TaxUnit node in Neo4j.

        Args:
            node: The LegalNode to create.
        """
        with self.driver.session(database=self.config.database) as session:
            session.run(
                """
                MERGE (n:TaxUnit {id: $id})
                SET n.identifier = $identifier,
                    n.type = $type,
                    n.num = $num,
                    n.heading = $heading,
                    n.text = $text,
                    n.status = $status,
                    n.path = $path,
                    n.is_container = $is_container,
                    n.is_content = $is_content
                """,
                id=node.id,
                identifier=node.identifier,
                type=node.node_type.value,
                num=node.num,
                heading=node.heading,
                text=node.text,
                status=node.status.value,
                path=node.hierarchical_path,
                is_container=node.is_container,
                is_content=node.is_content,
            )

    def create_nodes_batch(self, nodes: list[LegalNode], batch_size: int = 100) -> int:
        """
        Create multiple TaxUnit nodes in batches.

        Args:
            nodes: List of LegalNodes to create.
            batch_size: Number of nodes per batch.

        Returns:
            Total number of nodes created.
        """
        total = 0

        with self.driver.session(database=self.config.database) as session:
            for i in range(0, len(nodes), batch_size):
                batch = nodes[i:i + batch_size]
                node_data = [
                    {
                        "id": n.id,
                        "identifier": n.identifier,
                        "type": n.node_type.value,
                        "num": n.num,
                        "heading": n.heading,
                        "text": n.text,
                        "status": n.status.value,
                        "path": n.hierarchical_path,
                        "is_container": n.is_container,
                        "is_content": n.is_content,
                    }
                    for n in batch
                ]

                session.run(
                    """
                    UNWIND $nodes AS node
                    MERGE (n:TaxUnit {id: node.id})
                    SET n.identifier = node.identifier,
                        n.type = node.type,
                        n.num = node.num,
                        n.heading = node.heading,
                        n.text = node.text,
                        n.status = node.status,
                        n.path = node.path,
                        n.is_container = node.is_container,
                        n.is_content = node.is_content
                    """,
                    nodes=node_data,
                )

                total += len(batch)
                logger.debug(f"Created batch of {len(batch)} nodes (total: {total})")

        logger.info(f"Created {total} TaxUnit nodes")
        return total

    def create_parent_relationship(self, parent_id: str, child_id: str) -> None:
        """
        Create a PARENT_OF relationship between two nodes.

        Args:
            parent_id: ID of the parent node.
            child_id: ID of the child node.
        """
        with self.driver.session(database=self.config.database) as session:
            session.run(
                """
                MATCH (parent:TaxUnit {id: $parent_id})
                MATCH (child:TaxUnit {id: $child_id})
                MERGE (parent)-[:PARENT_OF]->(child)
                """,
                parent_id=parent_id,
                child_id=child_id,
            )

    def create_parent_relationships_batch(
        self, relationships: list[tuple[str, str]], batch_size: int = 100
    ) -> int:
        """
        Create multiple PARENT_OF relationships in batches.

        Args:
            relationships: List of (parent_id, child_id) tuples.
            batch_size: Number of relationships per batch.

        Returns:
            Total number of relationships created.
        """
        total = 0

        with self.driver.session(database=self.config.database) as session:
            for i in range(0, len(relationships), batch_size):
                batch = relationships[i:i + batch_size]
                rel_data = [
                    {"parent_id": parent_id, "child_id": child_id}
                    for parent_id, child_id in batch
                ]

                session.run(
                    """
                    UNWIND $rels AS rel
                    MATCH (parent:TaxUnit {id: rel.parent_id})
                    MATCH (child:TaxUnit {id: rel.child_id})
                    MERGE (parent)-[:PARENT_OF]->(child)
                    """,
                    rels=rel_data,
                )

                total += len(batch)
                logger.debug(f"Created batch of {len(batch)} PARENT_OF relationships (total: {total})")

        logger.info(f"Created {total} PARENT_OF relationships")
        return total

    def create_reference_relationship(
        self,
        source_id: str,
        target_section: str,
        reference_type: str = "general",
        context: str = "",
    ) -> bool:
        """
        Create a REFERENCES relationship between nodes.

        Args:
            source_id: ID of the source node.
            target_section: Section number being referenced (e.g., "162", "274(a)").
            reference_type: Type of reference (definition, exception, subject_to, general).
            context: Surrounding text containing the reference.

        Returns:
            True if relationship was created, False if target not found.
        """
        # Build possible target IDs
        # e.g., "162" -> "26 USC 162", "274(a)" -> "26 USC 274(a)"
        target_id = f"26 USC {target_section}"

        with self.driver.session(database=self.config.database) as session:
            result = session.run(
                """
                MATCH (source:TaxUnit {id: $source_id})
                MATCH (target:TaxUnit {id: $target_id})
                MERGE (source)-[r:REFERENCES]->(target)
                SET r.type = $ref_type,
                    r.context = $context
                RETURN target.id as found
                """,
                source_id=source_id,
                target_id=target_id,
                ref_type=reference_type,
                context=context[:500] if context else "",  # Truncate long context
            )

            record = result.single()
            return record is not None

    def create_reference_relationships_batch(
        self,
        references: list[tuple[str, Reference]],
        batch_size: int = 100,
    ) -> tuple[int, int]:
        """
        Create multiple REFERENCES relationships in batches.

        Args:
            references: List of (source_id, Reference) tuples.
            batch_size: Number of relationships per batch.

        Returns:
            Tuple of (created_count, not_found_count).
        """
        created = 0
        not_found = 0

        with self.driver.session(database=self.config.database) as session:
            for i in range(0, len(references), batch_size):
                batch = references[i:i + batch_size]
                ref_data = [
                    {
                        "source_id": source_id,
                        "target_id": f"26 USC {ref.target_section}",
                        "ref_type": ref.reference_type,
                        "context": ref.context[:500] if ref.context else "",
                    }
                    for source_id, ref in batch
                ]

                # Use OPTIONAL MATCH to handle missing targets
                result = session.run(
                    """
                    UNWIND $refs AS ref
                    MATCH (source:TaxUnit {id: ref.source_id})
                    OPTIONAL MATCH (target:TaxUnit {id: ref.target_id})
                    WITH source, target, ref
                    WHERE target IS NOT NULL
                    MERGE (source)-[r:REFERENCES]->(target)
                    SET r.type = ref.ref_type,
                        r.context = ref.context
                    RETURN count(r) as created
                    """,
                    refs=ref_data,
                )

                batch_created = result.single()["created"]
                created += batch_created
                not_found += len(batch) - batch_created

                logger.debug(
                    f"Created batch of {batch_created} REFERENCES relationships "
                    f"({len(batch) - batch_created} targets not found)"
                )

        logger.info(
            f"Created {created} REFERENCES relationships "
            f"({not_found} targets not found in graph)"
        )
        return created, not_found

    def get_node_count(self) -> int:
        """Get total count of TaxUnit nodes."""
        with self.driver.session(database=self.config.database) as session:
            result = session.run("MATCH (n:TaxUnit) RETURN count(n) as count")
            return result.single()["count"]

    def get_relationship_counts(self) -> dict[str, int]:
        """Get counts of each relationship type."""
        with self.driver.session(database=self.config.database) as session:
            result = session.run("""
                MATCH ()-[r]->()
                RETURN type(r) as type, count(r) as count
            """)
            return {record["type"]: record["count"] for record in result}

    def get_section_with_context(self, section_id: str, hops: int = 1) -> dict:
        """
        Get a section with its parent context and references.

        Args:
            section_id: The section ID (e.g., "26 USC 162").
            hops: Number of hops to traverse for references.

        Returns:
            Dictionary containing the section, parents, and references.
        """
        with self.driver.session(database=self.config.database) as session:
            result = session.run(
                """
                MATCH (section:TaxUnit {id: $section_id})

                // Get parent chain
                OPTIONAL MATCH path = (ancestor:TaxUnit)-[:PARENT_OF*]->(section)
                WITH section, collect(DISTINCT ancestor) as parents

                // Get direct references
                OPTIONAL MATCH (section)-[r:REFERENCES]->(ref:TaxUnit)
                WITH section, parents, collect(DISTINCT {
                    node: ref,
                    type: r.type,
                    context: r.context
                }) as refs

                // Get child subsections
                OPTIONAL MATCH (section)-[:PARENT_OF]->(child:TaxUnit)

                RETURN section,
                       parents,
                       refs as references,
                       collect(DISTINCT child) as children
                """,
                section_id=section_id,
            )

            record = result.single()
            if not record:
                return {}

            return {
                "section": dict(record["section"]),
                "parents": [dict(p) for p in record["parents"]],
                "references": [
                    {
                        "node": dict(r["node"]) if r["node"] else None,
                        "type": r["type"],
                        "context": r["context"],
                    }
                    for r in record["references"]
                    if r["node"]
                ],
                "children": [dict(c) for c in record["children"]],
            }

    def find_sections_referencing(self, section_id: str) -> list[dict]:
        """
        Find all sections that reference a given section.

        Args:
            section_id: The section ID to find references to.

        Returns:
            List of sections that reference the given section.
        """
        with self.driver.session(database=self.config.database) as session:
            result = session.run(
                """
                MATCH (source:TaxUnit)-[r:REFERENCES]->(target:TaxUnit {id: $section_id})
                RETURN source, r.type as ref_type, r.context as context
                ORDER BY source.id
                """,
                section_id=section_id,
            )

            return [
                {
                    "source": dict(record["source"]),
                    "reference_type": record["ref_type"],
                    "context": record["context"],
                }
                for record in result
            ]


def flatten_nodes(node: LegalNode) -> list[LegalNode]:
    """Recursively flatten a node tree into a list."""
    nodes = [node]
    for child in node.children:
        nodes.extend(flatten_nodes(child))
    return nodes


def extract_parent_relationships(node: LegalNode) -> list[tuple[str, str]]:
    """Extract all parent-child relationships from a node tree."""
    relationships = []
    for child in node.children:
        relationships.append((node.id, child.id))
        relationships.extend(extract_parent_relationships(child))
    return relationships


def extract_all_references(node: LegalNode) -> list[tuple[str, Reference]]:
    """Extract all references from a node tree."""
    references = [(node.id, ref) for ref in node.references]
    for child in node.children:
        references.extend(extract_all_references(child))
    return references


def ingest_tax_code(
    parsed: ParsedTaxCode,
    config: Optional[Neo4jConfig] = None,
    clear_existing: bool = False,
) -> dict:
    """
    Ingest a parsed tax code into Neo4j.

    Args:
        parsed: The parsed tax code structure.
        config: Neo4j configuration. If None, uses env vars.
        clear_existing: Whether to clear existing data first.

    Returns:
        Dictionary with ingestion statistics.
    """
    stats = {
        "nodes_created": 0,
        "parent_relationships": 0,
        "reference_relationships": 0,
        "references_not_found": 0,
    }

    with TaxCodeGraph(config) as graph:
        # Clear existing data if requested
        if clear_existing:
            graph.clear_database()

        # Create indexes
        graph.create_indexes()

        # Flatten the tree into a list of nodes
        logger.info("Flattening node tree...")
        all_nodes = flatten_nodes(parsed.root)
        logger.info(f"Found {len(all_nodes)} nodes to ingest")

        # Create all nodes
        logger.info("Creating TaxUnit nodes...")
        stats["nodes_created"] = graph.create_nodes_batch(all_nodes)

        # Extract and create parent relationships
        logger.info("Creating PARENT_OF relationships...")
        parent_rels = extract_parent_relationships(parsed.root)
        stats["parent_relationships"] = graph.create_parent_relationships_batch(parent_rels)

        # Extract and create reference relationships
        logger.info("Creating REFERENCES relationships...")
        all_refs = extract_all_references(parsed.root)
        created, not_found = graph.create_reference_relationships_batch(all_refs)
        stats["reference_relationships"] = created
        stats["references_not_found"] = not_found

        # Get final counts
        stats["total_nodes"] = graph.get_node_count()
        stats["relationship_counts"] = graph.get_relationship_counts()

    return stats
