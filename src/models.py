"""
Pydantic models for US Tax Code (Title 26) hierarchical structure.

The hierarchy follows:
Title > Subtitle > Chapter > Subchapter > Part > Subpart > Section > Subsection > Paragraph
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, computed_field


class NodeType(str, Enum):
    """Types of nodes in the tax code hierarchy."""
    TITLE = "title"
    SUBTITLE = "subtitle"
    CHAPTER = "chapter"
    SUBCHAPTER = "subchapter"
    PART = "part"
    SUBPART = "subpart"
    SECTION = "section"
    SUBSECTION = "subsection"
    PARAGRAPH = "paragraph"
    SUBPARAGRAPH = "subparagraph"
    CLAUSE = "clause"


class NodeStatus(str, Enum):
    """Status of a legal node."""
    ACTIVE = "active"
    REPEALED = "repealed"
    EXPIRED = "expired"
    RESERVED = "reserved"


class Reference(BaseModel):
    """A cross-reference to another section in the tax code."""
    target_section: str = Field(description="The section being referenced (e.g., '162', '274(a)(3)')")
    context: str = Field(description="The surrounding text containing the reference")
    reference_type: str = Field(default="general", description="Type of reference: 'definition', 'exception', 'subject_to', 'general'")

    def __hash__(self):
        return hash((self.target_section, self.reference_type))

    def __eq__(self, other):
        if not isinstance(other, Reference):
            return False
        return self.target_section == other.target_section and self.reference_type == other.reference_type


class LegalNode(BaseModel):
    """
    A node in the tax code hierarchy.

    This is the atomic unit for the GraphRAG system, containing:
    - Unique identifier (official citation)
    - Hierarchical path for context
    - Raw legal text
    - Cross-references to other sections
    - Child nodes for traversal
    """
    id: str = Field(description="Official citation (e.g., '26 USC 162(a)')")
    identifier: str = Field(description="XML identifier path (e.g., '/us/usc/t26/s162/a')")
    node_type: NodeType = Field(description="Type of this node in the hierarchy")
    num: Optional[str] = Field(default=None, description="Section/subsection number (e.g., '162', '(a)')")
    heading: Optional[str] = Field(default=None, description="Title/heading of this node")
    text: str = Field(default="", description="Raw legal text content (cleaned of XML tags)")
    status: NodeStatus = Field(default=NodeStatus.ACTIVE, description="Whether this section is active, repealed, etc.")

    # Hierarchy
    parent_id: Optional[str] = Field(default=None, description="ID of the parent node")
    children: list[LegalNode] = Field(default_factory=list, description="Child nodes")

    # Cross-references
    references: list[Reference] = Field(default_factory=list, description="Cross-references to other sections")

    # Metadata
    hierarchical_path: str = Field(default="", description="String path (e.g., 'Subtitle A > Chapter 1 > Section 162')")

    model_config = {"arbitrary_types_allowed": True}

    @computed_field
    @property
    def is_container(self) -> bool:
        """Returns True if this is a container node (Title, Subtitle, Chapter, etc.)."""
        return self.node_type in {
            NodeType.TITLE, NodeType.SUBTITLE, NodeType.CHAPTER,
            NodeType.SUBCHAPTER, NodeType.PART, NodeType.SUBPART
        }

    @computed_field
    @property
    def is_content(self) -> bool:
        """Returns True if this is a content node (Section, Subsection, Paragraph)."""
        return self.node_type in {
            NodeType.SECTION, NodeType.SUBSECTION, NodeType.PARAGRAPH,
            NodeType.SUBPARAGRAPH, NodeType.CLAUSE
        }

    @computed_field
    @property
    def is_leaf(self) -> bool:
        """Returns True if this node has no children."""
        return len(self.children) == 0

    @computed_field
    @property
    def full_text(self) -> str:
        """Returns text with heading prepended for context."""
        parts = []
        if self.heading:
            parts.append(self.heading)
        if self.text:
            parts.append(self.text)
        return " - ".join(parts) if parts else ""

    @computed_field
    @property
    def embedding_text(self) -> str:
        """
        Returns the text to be used for vector embedding.
        Includes hierarchical context for better semantic search.
        """
        context_parts = []
        if self.hierarchical_path:
            context_parts.append(self.hierarchical_path)
        if self.heading:
            context_parts.append(self.heading)
        if self.text:
            context_parts.append(self.text)
        return " > ".join(context_parts[:2]) + ": " + (context_parts[2] if len(context_parts) > 2 else "")

    def get_all_sections(self) -> list[LegalNode]:
        """Recursively get all section-level nodes."""
        sections = []
        if self.node_type == NodeType.SECTION:
            sections.append(self)
        for child in self.children:
            sections.extend(child.get_all_sections())
        return sections

    def get_all_leaf_nodes(self) -> list[LegalNode]:
        """Recursively get all leaf nodes (for embedding)."""
        if self.is_leaf:
            return [self]
        leaves = []
        for child in self.children:
            leaves.extend(child.get_all_leaf_nodes())
        return leaves

    def find_by_id(self, target_id: str) -> Optional[LegalNode]:
        """Find a node by its ID."""
        if self.id == target_id:
            return self
        for child in self.children:
            result = child.find_by_id(target_id)
            if result:
                return result
        return None

    def to_dict_flat(self) -> dict:
        """Convert to a flat dictionary (without nested children) for storage."""
        return {
            "id": self.id,
            "identifier": self.identifier,
            "node_type": self.node_type.value,
            "num": self.num,
            "heading": self.heading,
            "text": self.text,
            "status": self.status.value,
            "parent_id": self.parent_id,
            "hierarchical_path": self.hierarchical_path,
            "references": [ref.model_dump() for ref in self.references],
            "child_ids": [child.id for child in self.children],
        }


class ParsedTaxCode(BaseModel):
    """Container for the fully parsed tax code."""
    title: str = Field(default="Title 26 - Internal Revenue Code")
    root: LegalNode = Field(description="Root node of the parsed hierarchy")
    total_sections: int = Field(default=0, description="Total number of sections parsed")
    total_nodes: int = Field(default=0, description="Total number of nodes parsed")
    repealed_sections: list[str] = Field(default_factory=list, description="List of repealed section IDs")

    def get_section(self, section_num: str) -> Optional[LegalNode]:
        """Get a section by its number (e.g., '162', '274')."""
        target_id = f"26 USC {section_num}"
        return self.root.find_by_id(target_id)

    def get_all_sections(self) -> list[LegalNode]:
        """Get all section-level nodes."""
        return self.root.get_all_sections()

    def get_all_references(self) -> list[tuple[str, Reference]]:
        """Get all cross-references with their source node IDs."""
        refs = []

        def collect_refs(node: LegalNode):
            for ref in node.references:
                refs.append((node.id, ref))
            for child in node.children:
                collect_refs(child)

        collect_refs(self.root)
        return refs


# Reference detection patterns
SECTION_REFERENCE_PATTERNS = [
    # "section 162" or "Section 162"
    (r'\bsection\s+(\d+[A-Za-z]?(?:\([a-z0-9]+\))*)', 'general'),
    # "sections 162 and 274"
    (r'\bsections?\s+(\d+[A-Za-z]?)\s+(?:and|or)\s+(\d+[A-Za-z]?)', 'general'),
    # "sec. 162" or "Sec. 162"
    (r'\bsec\.\s*(\d+[A-Za-z]?(?:\([a-z0-9]+\))*)', 'general'),
    # "as defined in section 7701"
    (r'as defined in section\s+(\d+[A-Za-z]?(?:\([a-z0-9]+\))*)', 'definition'),
    # "subject to section 274" or "subject to the provisions of section 274"
    (r'subject to (?:the provisions of )?section\s+(\d+[A-Za-z]?(?:\([a-z0-9]+\))*)', 'subject_to'),
    # "except as provided in section 274"
    (r'except as provided in section\s+(\d+[A-Za-z]?(?:\([a-z0-9]+\))*)', 'exception'),
    # "under section 162"
    (r'under section\s+(\d+[A-Za-z]?(?:\([a-z0-9]+\))*)', 'general'),
    # "provided in section 162"
    (r'provided in section\s+(\d+[A-Za-z]?(?:\([a-z0-9]+\))*)', 'general'),
    # "see section 162"
    (r'see section\s+(\d+[A-Za-z]?(?:\([a-z0-9]+\))*)', 'general'),
]


def extract_references(text: str) -> list[Reference]:
    """
    Extract cross-references from legal text.

    Returns a list of Reference objects with the target section
    and the type of reference (definition, exception, subject_to, general).
    """
    if not text:
        return []

    references = []
    seen = set()

    for pattern, ref_type in SECTION_REFERENCE_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            # Get all captured groups (some patterns capture multiple sections)
            groups = match.groups()
            for section in groups:
                if section and section not in seen:
                    seen.add(section)
                    # Get context (surrounding 50 chars)
                    start = max(0, match.start() - 30)
                    end = min(len(text), match.end() + 30)
                    context = text[start:end].strip()

                    references.append(Reference(
                        target_section=section,
                        context=f"...{context}...",
                        reference_type=ref_type
                    ))

    return references
