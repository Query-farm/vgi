# Scalar argument monotonicity

VGI protocol 2.0 includes an optional `FunctionInfo.argument_monotonicity`
field immediately after `null_handling`. Its Arrow type is nullable
`list<utf8>` with these uppercase values:

- `UNKNOWN`
- `CONSTANT`
- `NON_DECREASING`
- `STRICTLY_INCREASING`
- `NON_INCREASING`
- `STRICTLY_DECREASING`

The field is valid only for scalar functions. `NULL` means the worker makes no
claims. A present list must contain one non-null value for every field in the
ordered `FunctionInfo.arguments` schema. Fixed, defaulted, and constant
arguments each occupy one declaration slot. A vararg declaration occupies one
slot, and its claim applies to every call-time expansion. Named invocation
order does not affect the list.

The current extension is built against DuckDB 1.5.x. It parses, validates, and
preserves this protocol metadata using VGI-owned types but does not pass it into
DuckDB's optimizer. Optimizer integration can be added when the extension moves
to a DuckDB version with the per-argument monotonicity API.
