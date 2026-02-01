# Engineering Documentation

This directory contains internal engineering documentation for pgqrs development.

## Structure

### `/adr/` - Architecture Decision Records
Numbered, immutable records of significant architectural decisions. Each ADR captures:
- What was decided
- Why it was decided
- What alternatives were considered
- What the consequences are

Format: `NNNN-title-in-kebab-case.md` (e.g., `0001-database-abstraction.md`)

### `/design/` - Detailed Design Documents
Comprehensive design documentation for major features and components. These are living documents that can be updated as implementations evolve.

Examples:
- Database abstraction layer design
- Backend-specific implementation details
- API design proposals

### `/reviews/` - Code Review Discussions
Outcomes and discussions from significant code reviews, particularly for major features or architectural changes.

### `/investigations/` - Research & Spike Results
Exploratory work, research findings, performance benchmarks, and spike results. Documents what was learned during investigation phases.

### `/processes/` - Engineering Processes
Team processes, workflows, and guidelines for development, testing, and releases.

## Root-Level Planning Docs
- `product-requirements.md` - High-level product requirements
- `implementation_plan.md` - Cross-cutting implementation plans

## Contributing

- **ADRs**: Use the template in `adr/template.md`. ADRs are immutable once accepted.
- **Design Docs**: Write comprehensive, technical design documentation.
- **Reviews**: Document significant findings and decisions from code reviews.
- **Investigations**: Capture research results, benchmarks, and learnings.
