#!/usr/bin/env python3
"""
Neo4j Ingestion Script for US Tax Code.

This script parses the USLM XML and ingests it into Neo4j,
creating the hierarchical graph structure with cross-references.

Usage:
    python -m src.ingest [--xml-path PATH] [--clear] [--neo4j-uri URI]

Environment Variables:
    NEO4J_URI: Neo4j connection URI (default: bolt://localhost:7687)
    NEO4J_USERNAME: Neo4j username (default: neo4j)
    NEO4J_PASSWORD: Neo4j password (default: password)
    NEO4J_DATABASE: Neo4j database name (default: neo4j)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .graph import Neo4jConfig, TaxCodeGraph, ingest_tax_code
from .parser import parse_tax_code

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def print_stats(stats: dict) -> None:
    """Print ingestion statistics."""
    print("\n" + "=" * 60)
    print("INGESTION STATISTICS")
    print("=" * 60)
    print(f"Nodes created:              {stats['nodes_created']}")
    print(f"PARENT_OF relationships:    {stats['parent_relationships']}")
    print(f"REFERENCES relationships:   {stats['reference_relationships']}")
    print(f"References not found:       {stats['references_not_found']}")
    print(f"\nTotal nodes in graph:       {stats['total_nodes']}")

    if stats.get("relationship_counts"):
        print("\nRelationship counts by type:")
        for rel_type, count in stats["relationship_counts"].items():
            print(f"  - {rel_type}: {count}")


def demo_queries(config: Neo4jConfig) -> None:
    """Run demo queries to showcase the graph structure."""
    print("\n" + "=" * 60)
    print("DEMO QUERIES")
    print("=" * 60)

    with TaxCodeGraph(config) as graph:
        # Query 1: Get Section 1 with context
        print("\n1. Section 1 (Tax imposed) with context:")
        print("-" * 40)
        result = graph.get_section_with_context("26 USC 1")
        if result:
            section = result["section"]
            print(f"   ID: {section.get('id')}")
            print(f"   Heading: {section.get('heading')}")
            print(f"   Type: {section.get('type')}")
            print(f"   Path: {section.get('path')}")

            if result["parents"]:
                print(f"\n   Parent hierarchy ({len(result['parents'])} levels):")
                for parent in sorted(result["parents"], key=lambda x: x.get("path", "")):
                    print(f"     - {parent.get('type')}: {parent.get('heading')}")

            if result["children"]:
                print(f"\n   Children ({len(result['children'])}):")
                for child in result["children"][:5]:
                    print(f"     - {child.get('num')}: {child.get('heading')}")

            if result["references"]:
                print(f"\n   References ({len(result['references'])}):")
                for ref in result["references"][:5]:
                    node = ref["node"]
                    print(f"     -> {node.get('id')} ({ref['type']})")
        else:
            print("   Section not found")

        # Query 2: Find what references Section 162
        print("\n2. Sections that reference Section 162:")
        print("-" * 40)
        refs = graph.find_sections_referencing("26 USC 162")
        if refs:
            for ref in refs[:10]:
                source = ref["source"]
                print(f"   - {source.get('id')}: {source.get('heading', 'N/A')[:50]}")
        else:
            print("   No references found (Section 162 may not be in sample data)")

        # Query 3: Get a subsection with full context
        print("\n3. Subsection 1(a) with full context:")
        print("-" * 40)
        result = graph.get_section_with_context("26 USC 1(a)")
        if result:
            section = result["section"]
            print(f"   Heading: {section.get('heading')}")
            text = section.get("text", "")[:200]
            if text:
                print(f"   Text: {text}...")

            if result["references"]:
                print(f"\n   Cross-references:")
                for ref in result["references"]:
                    node = ref["node"]
                    print(f"     -> Section {node.get('id')} ({ref['type']})")
        else:
            print("   Subsection not found")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Ingest US Tax Code into Neo4j graph database"
    )
    parser.add_argument(
        "--xml-path",
        type=Path,
        default=Path("sample_usc26.xml"),
        help="Path to the USLM XML file (default: sample_usc26.xml)",
    )
    parser.add_argument(
        "--max-sections",
        type=int,
        default=None,
        help="Maximum number of sections to parse (default: all)",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing TaxUnit data before ingestion",
    )
    parser.add_argument(
        "--neo4j-uri",
        type=str,
        default=None,
        help="Neo4j connection URI (overrides NEO4J_URI env var)",
    )
    parser.add_argument(
        "--neo4j-username",
        type=str,
        default=None,
        help="Neo4j username (overrides NEO4J_USERNAME env var)",
    )
    parser.add_argument(
        "--neo4j-password",
        type=str,
        default=None,
        help="Neo4j password (overrides NEO4J_PASSWORD env var)",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run demo queries after ingestion",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse XML but don't ingest into Neo4j",
    )

    args = parser.parse_args()

    # Build Neo4j config
    config = Neo4jConfig.from_env()
    if args.neo4j_uri:
        config.uri = args.neo4j_uri
    if args.neo4j_username:
        config.username = args.neo4j_username
    if args.neo4j_password:
        config.password = args.neo4j_password

    # Parse the XML
    print(f"Parsing: {args.xml_path}")
    if args.max_sections:
        print(f"Max sections: {args.max_sections}")

    try:
        parsed = parse_tax_code(args.xml_path, max_sections=args.max_sections)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Parse error: {e}", file=sys.stderr)
        raise

    print(f"Parsed {parsed.total_sections} sections ({parsed.total_nodes} total nodes)")

    if args.dry_run:
        print("\nDry run - skipping Neo4j ingestion")
        return 0

    # Ingest into Neo4j
    print(f"\nConnecting to Neo4j at {config.uri}...")
    try:
        stats = ingest_tax_code(parsed, config, clear_existing=args.clear)
        print_stats(stats)
    except Exception as e:
        print(f"Ingestion error: {e}", file=sys.stderr)
        logger.exception("Failed to ingest into Neo4j")
        return 1

    # Run demo queries if requested
    if args.demo:
        try:
            demo_queries(config)
        except Exception as e:
            print(f"Demo query error: {e}", file=sys.stderr)
            logger.exception("Failed to run demo queries")

    print("\nIngestion complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
