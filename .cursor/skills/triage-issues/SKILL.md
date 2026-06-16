---
name: triage-issues
description: |
  Analyzes a single GitHub issue at a time. Reads the description, defines labels and priority, researches additional information, and provides a short but detailed report. Use when the user asks to triage an issue, analyze a bug report, or categorize a GitHub issue.
disable-model-invocation: true
---

# Issue Triage Workflow

## Stage 1: Preview

1. Sizes the description into categories: Tiny, Small, Medium, Large. See [Description Sizes in references.md](references.md) for size definitions.
2. Analyzes based on description size:
   - **Tiny**: Tries to detect what module is affected. Uses specific terms and method names. JDBC, for example, has very recognizable sets of methods. Remembers or outputs additional information.
   - **Small or Medium**: Finds what module or functionality is affected and records this information. Small or Medium descriptions just need more technical details.
   - **Large**: Refines and compacts the issue for further investigation. Large descriptions should have enough data to pinpoint the problem but are often in a form hardly readable by humans, so creates a minimal version.
3. Checks that there is minimal data available about the problem. See [Minimal Issue Details in references.md](references.md).

## Stage 2: Research

Before exploring the tree, use [source-map.md](source-map.md) to locate the
affected module and `area:*` labels (module/package boundaries, label → source
location, entry-point classes, and stacktrace → module heuristics). Only grep
the source once the map has narrowed the scope.

Every issue type has its own research approach.

### Question

1. Looks up documentation first if there is related information. See [Module Documentation in references.md](references.md).
2. If the question is about how configuration works - explores source code. See [Module Sources in references.md](references.md).
3. If the question is about usage then generates a set of questions to get details about the use-case.
4. Else, notes that this question needs human attention.

### Potential Bug

1. Finds out scope: determines the module, what configuration parameters are involved, and what classes implement the area.
   1. If there is a stacktrace, adds the call chain of methods for review.
   2. If there is a specific code example given, looks at what functions are called in what order.
2. Understands the runtime environment more. Generates questions to discover more.
   1. If JDBC, then there could be some framework involved. Finds out what the conditions should be.
   2. If client, then the application environment is important.
3. When the user provides a data example or use case description, creates a test scenario or code. Keeps code minimal.

### What to ask User

- Asks user about specific version of client or JDBC driver. Asks about specific version of server.
- Asks about network setup if relevant. For example, issue related network may be affected by proxy in the middle.
- Asks about reproducibility of the problem if it is network related and looks like have unstable nature.

## Stage 3: Summary

When finishing the analysis, outputs the findings using this exact template:
```markdown
## Triage Report
**Size**: [Tiny/Small/Medium/Large]
**Type**: [Question/Bug]
**Affected Module**: [Module Name]
### Summary
[1-2 sentences summarizing the core issue]
### Recommended Labels & Priority
- Labels: [label1, label2]
### Missing Information / Questions for User
1. [Question 1]
2. [Question 2]
### Tests to Add

**Test 1: Scenario**
```
// code
```


**Test 2: Scenario**
```
// code
```
```
