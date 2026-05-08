<table border="0" cellspacing="0" cellpadding="0">
  <tr>
    <td><img src="https://github.com/LerianStudio.png" width="72" alt="Lerian" /></td>
    <td><h1>lib-streaming</h1></td>
  </tr>
</table>

---

## Description

<!-- Summarize what this PR changes and why. Mention the affected surface
     (producer, publish, outbox, metrics, health, lifecycle, streaming). -->

## Type of Change

- [ ] `feat`: New feature or capability
- [ ] `fix`: Bug fix
- [ ] `perf`: Performance improvement
- [ ] `refactor`: Internal restructuring with no behavior change
- [ ] `docs`: Documentation only (README, docs/, llms.txt, inline comments)
- [ ] `style`: Formatting, whitespace, naming (no logic change)
- [ ] `test`: Adding or updating tests
- [ ] `ci`: CI pipeline or workflow changes
- [ ] `build`: Build system, Dockerfile, Go module dependencies
- [ ] `chore`: Maintenance, config, tooling
- [ ] `revert`: Reverts a previous commit
- [ ] `BREAKING CHANGE`: Consumers must update their integration

## Breaking Changes

<!-- If applicable, describe exactly what breaks (env vars, route paths,
     persistence schema, public ports/adapters) and how downstream services
     should migrate. Remove this section if not applicable. -->

None.

## Testing

- [ ] Unit tests pass (`go test -tags=unit ./...`)
- [ ] Integration tests pass if integration paths are exercised
- [ ] Lint passes

**Test evidence / Actions run:** <!-- Optional: link to a CI run or screenshot -->

## Architectural Checklist

- [ ] No `panic()` in production paths — uses wrapped errors
- [ ] Timestamps use `time.Now().UTC()`
- [ ] Errors wrapped with `%w`
- [ ] Public API additions documented and covered by tests
- [ ] No breaking changes to exported types/options without `BREAKING CHANGE`

## Related Issues

Closes #
