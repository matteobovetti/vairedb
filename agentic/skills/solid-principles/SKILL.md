---
name: SOLID Principles
description: This skill should be used when the user asks about "SOLID principles", "single responsibility", "open/closed principle", "Liskov substitution", "interface segregation", "dependency inversion", or when analyzing code for design principle violations. Provides comprehensive guidance for detecting and fixing SOLID violations.
version: 0.1.0
---

# SOLID Principles Guide

## Overview

SOLID is a set of five object-oriented design principles that promote maintainable, flexible, and scalable code. This skill provides guidance for detecting violations and applying correct patterns across multiple languages.

## The Five Principles

### S - Single Responsibility Principle (SRP)

A class should have only one reason to change.

**Detection Patterns**:
- Classes with multiple unrelated methods
- Files exceeding 200-300 lines
- Class names containing "And", "Manager", "Handler" doing too much
- Methods that mix I/O, business logic, and presentation

**Refactoring Strategy**:
- Extract cohesive functionality into separate classes
- Use composition to combine smaller components
- Apply facade pattern for unified interfaces

### O - Open/Closed Principle (OCP)

Software entities should be open for extension but closed for modification.

**Detection Patterns**:
- Switch statements on type that grow with new types
- Repeated if/else chains checking object types
- Modifications to existing code for new features

**Refactoring Strategy**:
- Use polymorphism and inheritance
- Apply strategy pattern for varying behaviors
- Implement plugin architectures

### L - Liskov Substitution Principle (LSP)

Subtypes must be substitutable for their base types.

**Detection Patterns**:
- Overridden methods throwing unexpected exceptions
- Subclasses that don't use inherited methods
- Type checks before calling base type methods
- Empty or no-op implementations of inherited methods

**Refactoring Strategy**:
- Favor composition over inheritance
- Use interface segregation
- Create proper type hierarchies

### I - Interface Segregation Principle (ISP)

Clients should not be forced to depend on interfaces they don't use.

**Detection Patterns**:
- Interfaces with many methods (>5-7)
- Classes implementing interfaces with unused methods
- "Fat" interfaces that try to do everything

**Refactoring Strategy**:
- Split large interfaces into smaller, focused ones
- Use role interfaces
- Apply interface composition

### D - Dependency Inversion Principle (DIP)

High-level modules should not depend on low-level modules; both should depend on abstractions.

**Detection Patterns**:
- Direct instantiation of concrete classes
- Hard-coded dependencies
- Import of implementation details in high-level modules

**Refactoring Strategy**:
- Introduce interfaces/abstractions
- Use dependency injection
- Apply factory patterns

## Violation Severity Levels

| Severity | Description | Action |
|----------|-------------|--------|
| Critical | Principle completely ignored, major maintenance issues | Immediate refactoring required |
| High | Clear violation affecting multiple areas | Schedule refactoring soon |
| Medium | Partial violation, localized impact | Refactor during related changes |
| Low | Minor deviation, minimal impact | Note for future improvement |

## Analysis Workflow

To analyze code for SOLID violations:

1. **Scan for SRP violations first** - Large files and multi-purpose classes
2. **Check inheritance hierarchies** - LSP and OCP violations
3. **Examine interfaces** - ISP violations in interface definitions
4. **Trace dependencies** - DIP violations in module imports
5. **Document findings** with severity and refactoring suggestions

## Output Format

When reporting SOLID violations, structure findings as:

```markdown
## SOLID Analysis Results

### Critical Violations

#### [File:Line] Principle Violated
- **Issue**: Description of the problem
- **Impact**: Why this matters
- **Suggestion**: How to fix it
- **Example**: Code snippet showing fix

### High Severity

...
```
