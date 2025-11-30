# US Tax Code Hierarchical GraphRAG

A Hierarchical Retrieval-Augmented Generation (GraphRAG) system for the U.S. Internal Revenue Code (Title 26).

## Overview

This project parses the USLM XML format of the US Tax Code and creates a hierarchical graph database that preserves:
- **Legal hierarchy**: Title > Subtitle > Chapter > Subchapter > Part > Section > Subsection > Paragraph
- **Cross-references**: Edges between sections that reference each other ("as defined in section...", "subject to section...")

## Features

### Phase 1: XML Parsing
- Parses USLM XML format from the Office of the Law Revision Counsel (OLRC)
- Extracts hierarchical structure with Pydantic models
- Detects cross-references using regex patterns
- Handles edge cases: repealed sections, tables (converted to markdown)

### Phase 2: Neo4j Graph Ingestion
- Creates `TaxUnit` nodes with properties: id, type, heading, text, path, status
- Creates `PARENT_OF` relationships for hierarchy traversal
- Creates `REFERENCES` relationships for cross-reference navigation
- Batch processing for efficient ingestion
- Demo queries for graph exploration

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Parse XML (Phase 1)

```bash
# Parse and display statistics
python -m src.main --xml-path sample_usc26.xml --max-sections 50

# Show hierarchy tree
python -m src.main --xml-path sample_usc26.xml --show-hierarchy

# Show specific section details
python -m src.main --xml-path sample_usc26.xml --show-section 162

# Export to JSON
python -m src.main --xml-path sample_usc26.xml --output parsed.json
```

### Ingest to Neo4j (Phase 2)

```bash
# Start Neo4j (using Docker)
docker-compose up -d

# Or use docker run
docker run -d --name taxcode-neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/taxcode123 \
  neo4j:5.15-community

# Ingest data
python -m src.ingest --xml-path sample_usc26.xml --clear --demo

# Dry run (parse only, no Neo4j)
python -m src.ingest --xml-path sample_usc26.xml --dry-run
```

### Environment Variables

```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USERNAME="neo4j"
export NEO4J_PASSWORD="taxcode123"
export NEO4J_DATABASE="neo4j"
```

## Graph Schema

### Node: TaxUnit

| Property | Type | Description |
|----------|------|-------------|
| id | string | Official citation (e.g., "26 USC 162(a)") |
| identifier | string | XML path (e.g., "/us/usc/t26/s162/a") |
| type | string | Node type (section, subsection, paragraph, etc.) |
| num | string | Section/subsection number |
| heading | string | Title/heading |
| text | string | Legal text content |
| status | string | active, repealed, expired |
| path | string | Hierarchical path |

### Relationship: PARENT_OF

```
(Subtitle A)-[:PARENT_OF]->(Chapter 1)
(Chapter 1)-[:PARENT_OF]->(Section 162)
(Section 162)-[:PARENT_OF]->(Subsection (a))
```

### Relationship: REFERENCES

| Property | Type | Description |
|----------|------|-------------|
| type | string | Reference type (definition, exception, subject_to, general) |
| context | string | Surrounding text containing the reference |

```
(Section 162)-[:REFERENCES {type: "exception"}]->(Section 274)
```

## Example Cypher Queries

```cypher
-- Find a section with its parent hierarchy
MATCH path = (ancestor:TaxUnit)-[:PARENT_OF*]->(section:TaxUnit {id: "26 USC 162"})
RETURN path

-- Find all sections that reference Section 274
MATCH (source:TaxUnit)-[r:REFERENCES]->(target:TaxUnit {id: "26 USC 274"})
RETURN source.id, source.heading, r.type

-- Get section with context and references (for RAG)
MATCH (section:TaxUnit {id: "26 USC 162"})
OPTIONAL MATCH (parent:TaxUnit)-[:PARENT_OF*]->(section)
OPTIONAL MATCH (section)-[r:REFERENCES]->(ref:TaxUnit)
RETURN section, collect(DISTINCT parent) as parents, collect(DISTINCT {ref: ref, type: r.type}) as refs
```

## Running Tests

```bash
python -m unittest tests.test_graph -v
```

## Project Structure

```
Tax-code/
├── src/
│   ├── __init__.py
│   ├── models.py      # Pydantic models (LegalNode, Reference, etc.)
│   ├── parser.py      # USLM XML parser
│   ├── graph.py       # Neo4j integration
│   ├── ingest.py      # Ingestion CLI script
│   └── main.py        # Parser CLI script
├── tests/
│   ├── __init__.py
│   └── test_graph.py  # Unit tests
├── sample_usc26.xml   # Sample USLM XML (43 sections)
├── docker-compose.yml # Neo4j Docker setup
├── requirements.txt
└── README.md
```

## Data Source

- **Primary Source**: Office of the Law Revision Counsel (OLRC)
- **Format**: USLM XML (United States Legislative Markup)
- **Download**: https://uscode.house.gov/download/download.shtml

## Next Steps (Phase 3: RAG Pipeline)

1. Set up vector embeddings with hierarchical context injection
2. Implement hybrid search (vector + keyword)
3. Graph expansion for context assembly
4. LLM integration with citation requirements
