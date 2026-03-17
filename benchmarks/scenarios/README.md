# Scenario Specs

Scenario files are declarative workload descriptions.

They should define:

- benchmark ID
- question
- backends
- bindings
- profiles
- action
- answer

The `question` should state what we want to learn.

The `action` should describe the experiment used to answer the question.

That includes:

- fixed workload settings
- variables or sweeps

The `answer` should describe the result shape we expect.

That includes:

- the primary reported value or family of values
- the series or curves that constitute the answer
- which variables those curves vary by

They should not define executor logic.

Copy one of the templates in `queue/` or `workflow/` when starting a new scenario.
