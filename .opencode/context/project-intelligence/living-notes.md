<!-- Context: project-intelligence/living-notes | Priority: high | Version: 1.0 | Updated: 2026-02-19 -->

# Living Notes

> Active issues, technical debt, and open questions for this project.

## Quick Reference

- **Purpose**: Track active issues and gaps needing follow-up
- **Update When**: New issues are identified or resolved
- **Related Files**: `decisions-log.md`, `technical-domain.md`

## Active Issues

- **Python bindings API gaps**: Python surface missing/extra APIs relative to Rust (notably `Store.bootstrap`, workflow table access, `Consumer.dequeue_many_with_delay/dequeue_at`, `Admin` surface, `Archive.filter_by_fk`, and workflow run/step record access). Issue filed: "Python bindings missing APIs vs Rust".

## Related Files

- `decisions-log.md` - Decisions impacting API surface
- `technical-domain.md` - Architecture context
