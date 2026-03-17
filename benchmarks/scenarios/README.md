# Scenario Specs

Scenario files are declarative workload descriptions.

They should define:

- benchmark ID
- goal
- backends
- bindings
- profiles
- workload parameters
- primary metric

They should not define executor logic.

Copy one of the templates in `queue/` or `workflow/` when starting a new scenario.
