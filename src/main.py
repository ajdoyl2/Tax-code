#!/usr/bin/env python3
"""
Main script to parse and analyze US Tax Code (Title 26).

Usage:
    python -m src.main [--xml-path PATH] [--max-sections N]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .models import LegalNode, NodeStatus, NodeType, ParsedTaxCode
from .parser import parse_tax_code


def print_hierarchy(node: LegalNode, indent: int = 0, max_depth: int = 4) -> None:
    """Print the hierarchy of nodes in a tree format."""
    if indent > max_depth * 2:
        return

    prefix = "  " * indent
    status_marker = " [REPEALED]" if node.status == NodeStatus.REPEALED else ""

    # Format the node display
    if node.heading:
        display = f"{node.node_type.value}: {node.num or ''} - {node.heading}{status_marker}"
    else:
        display = f"{node.node_type.value}: {node.num or node.id}{status_marker}"

    print(f"{prefix}{display}")

    # Show references if any
    if node.references and indent <= max_depth * 2:
        for ref in node.references[:3]:  # Limit to first 3 refs
            print(f"{prefix}  -> References: Section {ref.target_section} ({ref.reference_type})")

    # Recurse into children
    for child in node.children:
        print_hierarchy(child, indent + 1, max_depth)


def print_statistics(parsed: ParsedTaxCode) -> None:
    """Print statistics about the parsed tax code."""
    print("\n" + "=" * 60)
    print("PARSING STATISTICS")
    print("=" * 60)
    print(f"Total nodes parsed: {parsed.total_nodes}")
    print(f"Total sections parsed: {parsed.total_sections}")
    print(f"Repealed sections: {len(parsed.repealed_sections)}")

    if parsed.repealed_sections:
        print(f"  Examples: {', '.join(parsed.repealed_sections[:5])}")

    # Count references
    all_refs = parsed.get_all_references()
    print(f"Total cross-references found: {len(all_refs)}")

    # Group references by type
    ref_types: dict[str, int] = {}
    for _, ref in all_refs:
        ref_types[ref.reference_type] = ref_types.get(ref.reference_type, 0) + 1

    if ref_types:
        print("Reference types:")
        for ref_type, count in sorted(ref_types.items(), key=lambda x: -x[1]):
            print(f"  - {ref_type}: {count}")

    # Count by node type
    def count_by_type(node: LegalNode, counts: dict[str, int]) -> None:
        counts[node.node_type.value] = counts.get(node.node_type.value, 0) + 1
        for child in node.children:
            count_by_type(child, counts)

    type_counts: dict[str, int] = {}
    count_by_type(parsed.root, type_counts)

    print("\nNodes by type:")
    for node_type, count in sorted(type_counts.items()):
        print(f"  - {node_type}: {count}")


def print_section_details(node: LegalNode) -> None:
    """Print detailed information about a section."""
    print(f"\n{'=' * 60}")
    print(f"SECTION: {node.id}")
    print(f"{'=' * 60}")
    print(f"Heading: {node.heading}")
    print(f"Path: {node.hierarchical_path}")
    print(f"Status: {node.status.value}")

    if node.text:
        # Truncate long text
        text = node.text[:500] + "..." if len(node.text) > 500 else node.text
        print(f"\nText:\n{text}")

    if node.references:
        print(f"\nCross-references ({len(node.references)}):")
        for ref in node.references:
            print(f"  - Section {ref.target_section} ({ref.reference_type})")
            print(f"    Context: {ref.context}")

    if node.children:
        print(f"\nSubsections ({len(node.children)}):")
        for child in node.children:
            print(f"  - {child.num}: {child.heading or '(no heading)'}")


def export_to_json(parsed: ParsedTaxCode, output_path: Path) -> None:
    """Export the parsed structure to JSON for inspection."""
    def node_to_dict(node: LegalNode) -> dict:
        return {
            "id": node.id,
            "identifier": node.identifier,
            "type": node.node_type.value,
            "num": node.num,
            "heading": node.heading,
            "text": node.text[:200] + "..." if len(node.text) > 200 else node.text,
            "status": node.status.value,
            "hierarchical_path": node.hierarchical_path,
            "references": [
                {"target": ref.target_section, "type": ref.reference_type}
                for ref in node.references
            ],
            "children": [node_to_dict(child) for child in node.children],
        }

    data = {
        "title": parsed.title,
        "total_sections": parsed.total_sections,
        "total_nodes": parsed.total_nodes,
        "repealed_sections": parsed.repealed_sections,
        "root": node_to_dict(parsed.root),
    }

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nExported to: {output_path}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse US Tax Code (Title 26) from USLM XML format"
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
        default=50,
        help="Maximum number of sections to parse (default: 50)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path for JSON export",
    )
    parser.add_argument(
        "--show-hierarchy",
        action="store_true",
        help="Print the hierarchy tree",
    )
    parser.add_argument(
        "--show-section",
        type=str,
        default=None,
        help="Show details for a specific section (e.g., '162')",
    )

    args = parser.parse_args()

    # Parse the XML
    print(f"Parsing: {args.xml_path}")
    print(f"Max sections: {args.max_sections}")
    print()

    try:
        parsed = parse_tax_code(args.xml_path, max_sections=args.max_sections)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Parse error: {e}", file=sys.stderr)
        raise

    # Print statistics
    print_statistics(parsed)

    # Show hierarchy if requested
    if args.show_hierarchy:
        print("\n" + "=" * 60)
        print("HIERARCHY")
        print("=" * 60)
        print_hierarchy(parsed.root, max_depth=5)

    # Show specific section if requested
    if args.show_section:
        section = parsed.get_section(args.show_section)
        if section:
            print_section_details(section)
        else:
            print(f"\nSection {args.show_section} not found")

    # Export to JSON if requested
    if args.output:
        export_to_json(parsed, args.output)

    # Show a few sample sections
    print("\n" + "=" * 60)
    print("SAMPLE SECTIONS (first 5)")
    print("=" * 60)

    sections = parsed.get_all_sections()[:5]
    for section in sections:
        status = f" [{section.status.value.upper()}]" if section.status != NodeStatus.ACTIVE else ""
        refs = f" ({len(section.references)} refs)" if section.references else ""
        print(f"  {section.id}: {section.heading}{status}{refs}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
