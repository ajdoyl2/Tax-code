"""
XML Parser for US Tax Code (Title 26) in USLM format.

This module parses the USLM XML format used by the Office of the
Law Revision Counsel (OLRC) into structured LegalNode objects.
"""

from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Optional

from lxml import etree

from .models import (
    LegalNode,
    NodeStatus,
    NodeType,
    ParsedTaxCode,
    Reference,
    extract_references,
)

# USLM namespace
USLM_NS = "http://xml.house.gov/schemas/uslm/1.0"
NAMESPACES = {"uslm": USLM_NS}

# Mapping from XML element names to NodeType
ELEMENT_TO_NODE_TYPE = {
    "title": NodeType.TITLE,
    "subtitle": NodeType.SUBTITLE,
    "chapter": NodeType.CHAPTER,
    "subchapter": NodeType.SUBCHAPTER,
    "part": NodeType.PART,
    "subpart": NodeType.SUBPART,
    "section": NodeType.SECTION,
    "subsection": NodeType.SUBSECTION,
    "paragraph": NodeType.PARAGRAPH,
    "subparagraph": NodeType.SUBPARAGRAPH,
    "clause": NodeType.CLAUSE,
}

# Elements that are containers (can have child structural elements)
CONTAINER_ELEMENTS = {"title", "subtitle", "chapter", "subchapter", "part", "subpart"}

# Elements that are content units
CONTENT_ELEMENTS = {"section", "subsection", "paragraph", "subparagraph", "clause"}


class USLMParser:
    """
    Parser for USLM XML format.

    Traverses the XML tree and builds a hierarchy of LegalNode objects,
    extracting metadata, text content, and cross-references.
    """

    def __init__(self, max_sections: Optional[int] = None):
        """
        Initialize the parser.

        Args:
            max_sections: Maximum number of sections to parse (for testing).
                         If None, parses all sections.
        """
        self.max_sections = max_sections
        self.sections_parsed = 0
        self.total_nodes = 0
        self.repealed_sections: list[str] = []

    def parse_file(self, xml_path: str | Path) -> ParsedTaxCode:
        """
        Parse a USLM XML file into a ParsedTaxCode structure.

        Args:
            xml_path: Path to the XML file.

        Returns:
            ParsedTaxCode containing the parsed hierarchy.
        """
        xml_path = Path(xml_path)
        if not xml_path.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")

        tree = etree.parse(str(xml_path))
        root = tree.getroot()

        # Find the main content (title element)
        # Try with namespace first, then without
        main = root.find(".//uslm:main", NAMESPACES)
        if main is None:
            main = root.find(".//main")

        title_elem = None
        if main is not None:
            title_elem = main.find("uslm:title", NAMESPACES)
            if title_elem is None:
                title_elem = main.find("title")

        if title_elem is None:
            # Try finding title directly under root
            title_elem = root.find(".//uslm:title", NAMESPACES)
            if title_elem is None:
                title_elem = root.find(".//title")

        if title_elem is None:
            raise ValueError("Could not find title element in XML")

        # Parse the title and its descendants
        root_node = self._parse_element(title_elem, parent_path="")

        if root_node is None:
            raise ValueError("Failed to parse root title element")

        return ParsedTaxCode(
            title="Title 26 - Internal Revenue Code",
            root=root_node,
            total_sections=self.sections_parsed,
            total_nodes=self.total_nodes,
            repealed_sections=self.repealed_sections,
        )

    def _parse_element(
        self,
        elem: etree._Element,
        parent_path: str,
        parent_id: Optional[str] = None,
    ) -> Optional[LegalNode]:
        """
        Recursively parse an XML element into a LegalNode.

        Args:
            elem: The XML element to parse.
            parent_path: The hierarchical path of the parent.
            parent_id: The ID of the parent node.

        Returns:
            LegalNode or None if element should be skipped.
        """
        # Check if we've hit the section limit
        if self.max_sections and self.sections_parsed >= self.max_sections:
            return None

        # Get the local name (without namespace)
        tag = etree.QName(elem.tag).localname if isinstance(elem.tag, str) else elem.tag

        # Skip elements that aren't part of the hierarchy
        if tag not in ELEMENT_TO_NODE_TYPE:
            return None

        node_type = ELEMENT_TO_NODE_TYPE[tag]

        # Extract identifier
        identifier = elem.get("identifier", "")

        # Extract num and heading
        num = self._get_text_content(elem, "num")
        heading = self._get_text_content(elem, "heading")

        # Determine status
        status = self._determine_status(elem)

        # Build the hierarchical path
        path_parts = []
        if parent_path:
            path_parts.append(parent_path)
        if heading:
            path_parts.append(f"{node_type.value.capitalize()}: {heading}")
        elif num:
            path_parts.append(f"{node_type.value.capitalize()} {num}")
        hierarchical_path = " > ".join(path_parts)

        # Build the official citation ID
        node_id = self._build_citation_id(node_type, num, identifier)

        # Extract text content
        text = self._extract_text(elem)

        # Extract cross-references
        references = extract_references(text)

        # Track sections
        if node_type == NodeType.SECTION:
            self.sections_parsed += 1
            if status == NodeStatus.REPEALED:
                self.repealed_sections.append(node_id)

        self.total_nodes += 1

        # Create the node
        node = LegalNode(
            id=node_id,
            identifier=identifier,
            node_type=node_type,
            num=num,
            heading=heading,
            text=text,
            status=status,
            parent_id=parent_id,
            hierarchical_path=hierarchical_path,
            references=references,
            children=[],
        )

        # Parse children
        for child_elem in elem:
            child_tag = etree.QName(child_elem.tag).localname if isinstance(child_elem.tag, str) else child_elem.tag
            if child_tag in ELEMENT_TO_NODE_TYPE:
                child_node = self._parse_element(
                    child_elem,
                    parent_path=hierarchical_path,
                    parent_id=node_id,
                )
                if child_node:
                    node.children.append(child_node)

        return node

    def _get_text_content(self, elem: etree._Element, child_name: str) -> Optional[str]:
        """Get the text content of a child element."""
        # Try with namespace
        child = elem.find(f"uslm:{child_name}", NAMESPACES)
        if child is None:
            child = elem.find(child_name)

        if child is not None:
            # Get all text including tail
            text = "".join(child.itertext()).strip()
            return text if text else None
        return None

    def _determine_status(self, elem: etree._Element) -> NodeStatus:
        """Determine the status of a node (active, repealed, etc.)."""
        # Check status attribute
        status_attr = elem.get("status", "").lower()
        if "repeal" in status_attr:
            return NodeStatus.REPEALED
        if "expired" in status_attr:
            return NodeStatus.EXPIRED
        if "reserved" in status_attr:
            return NodeStatus.RESERVED

        # Check text content for [Repealed] markers
        text = self._extract_text(elem).lower()
        if "[repealed" in text or "repealed." in text:
            return NodeStatus.REPEALED
        if "[expired" in text:
            return NodeStatus.EXPIRED

        return NodeStatus.ACTIVE

    def _extract_text(self, elem: etree._Element) -> str:
        """
        Extract and clean the text content from an element.

        This includes handling:
        - Nested content/text elements
        - Tables (converted to markdown)
        - Removal of structural child elements
        """
        text_parts = []

        # Get direct content element
        content = elem.find("uslm:content", NAMESPACES)
        if content is None:
            content = elem.find("content")

        if content is not None:
            text_parts.append(self._element_to_text(content))
        else:
            # Get text from the element itself, excluding structural children
            for child in elem:
                child_tag = etree.QName(child.tag).localname if isinstance(child.tag, str) else child.tag
                # Skip structural elements and metadata elements
                if child_tag not in ELEMENT_TO_NODE_TYPE and child_tag not in {"num", "heading", "meta"}:
                    text_parts.append(self._element_to_text(child))

            # Also get direct text
            if elem.text:
                text_parts.insert(0, elem.text.strip())

        # Clean up the text
        text = " ".join(filter(None, text_parts))
        text = self._clean_text(text)
        return text

    def _element_to_text(self, elem: etree._Element) -> str:
        """
        Convert an element to text, handling special cases like tables.
        """
        # Check if this is a table
        tag = etree.QName(elem.tag).localname if isinstance(elem.tag, str) else elem.tag

        if tag == "table":
            return self._table_to_markdown(elem)

        # Check for nested table
        table = elem.find(".//table")
        if table is None:
            table = elem.find(".//{http://www.w3.org/1999/xhtml}table")
        if table is not None:
            # Get text before and after table
            pre_text = elem.text or ""
            table_text = self._table_to_markdown(table)
            # Get remaining text
            post_parts = []
            for child in elem:
                if child.tail:
                    post_parts.append(child.tail)
            post_text = " ".join(post_parts)
            return f"{pre_text}\n{table_text}\n{post_text}".strip()

        # Regular text extraction
        return "".join(elem.itertext())

    def _table_to_markdown(self, table: etree._Element) -> str:
        """
        Convert an HTML table to markdown format.
        """
        rows = []

        # Find all rows (tr elements)
        for tr in table.iter():
            tr_tag = etree.QName(tr.tag).localname if isinstance(tr.tag, str) else tr.tag
            if tr_tag != "tr":
                continue

            cells = []
            for cell in tr:
                cell_tag = etree.QName(cell.tag).localname if isinstance(cell.tag, str) else cell.tag
                if cell_tag in ("th", "td"):
                    cell_text = "".join(cell.itertext()).strip()
                    cell_text = cell_text.replace("|", "\\|")  # Escape pipes
                    cells.append(cell_text)

            if cells:
                rows.append(cells)

        if not rows:
            return ""

        # Build markdown table
        md_lines = []

        # Header row
        if rows:
            md_lines.append("| " + " | ".join(rows[0]) + " |")
            # Separator
            md_lines.append("| " + " | ".join(["---"] * len(rows[0])) + " |")
            # Data rows
            for row in rows[1:]:
                # Pad row if needed
                while len(row) < len(rows[0]):
                    row.append("")
                md_lines.append("| " + " | ".join(row) + " |")

        return "\n".join(md_lines)

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text."""
        # Decode HTML entities
        text = html.unescape(text)

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text)

        # Remove leading/trailing whitespace
        text = text.strip()

        return text

    def _build_citation_id(
        self, node_type: NodeType, num: Optional[str], identifier: str
    ) -> str:
        """
        Build an official citation ID (e.g., "26 USC 162(a)").
        """
        if not identifier:
            return f"26 USC {num or 'unknown'}"

        # Parse the identifier path
        # Format: /us/usc/t26/s162/a -> 26 USC 162(a)
        parts = identifier.strip("/").split("/")

        if len(parts) >= 4 and parts[0] == "us" and parts[1] == "usc":
            # Extract section number from parts like "s162", "s25A"
            section_parts = []
            in_section = False

            for part in parts[3:]:  # Skip us/usc/t26
                if part.startswith("s"):
                    in_section = True
                    section_parts.append(part[1:])  # Remove 's' prefix
                elif part.startswith("st"):
                    # Subtitle
                    continue
                elif part.startswith("ch"):
                    # Chapter
                    continue
                elif part.startswith("sch"):
                    # Subchapter
                    continue
                elif part.startswith("pt"):
                    # Part
                    continue
                elif part.startswith("spt"):
                    # Subpart
                    continue
                elif in_section:
                    # Subsection/paragraph (a, b, 1, 2, etc.)
                    section_parts.append(f"({part})")

            if section_parts:
                return f"26 USC {''.join(section_parts)}"

        # Fallback
        if num:
            # Clean up num (remove "Sec. " prefix if present)
            clean_num = re.sub(r"^Sec\.?\s*", "", num)
            return f"26 USC {clean_num}"

        return f"26 USC {identifier}"


def parse_tax_code(
    xml_path: str | Path,
    max_sections: Optional[int] = None,
) -> ParsedTaxCode:
    """
    Convenience function to parse a tax code XML file.

    Args:
        xml_path: Path to the USLM XML file.
        max_sections: Maximum number of sections to parse (for testing).

    Returns:
        ParsedTaxCode containing the parsed hierarchy.
    """
    parser = USLMParser(max_sections=max_sections)
    return parser.parse_file(xml_path)
