# OpenCode Agent Configuration

This project uses custom agent configurations for development.

## Agent Personas

Load role-specific guidelines from `agents/personas/`:

- **Planner** (`agents/personas/planner.md`) - Planning and design phase
- **Builder** (`agents/personas/builder.md`) - Implementation phase
- **Tester** (`agents/personas/tester.md`) - Testing phase
- **Reviewer** (`agents/personas/reviewer.md`) - Code review phase

## Project Processes

See `agents/agents.md` for complete project processes, conventions, and workflows.

## Usage

When OpenCode agents work on this project, they will:
1. Load project processes from `agents/agents.md`
2. Load role-specific guidelines from `agents/personas/{role}.md`
3. Follow approval gates and safety standards
4. Ensure backend compatibility (Postgres, SQLite, Turso)

## Integration

This configuration complements global OpenCode settings in `~/.config/opencode/`.
Project-specific personas take precedence for pgqrs development.
