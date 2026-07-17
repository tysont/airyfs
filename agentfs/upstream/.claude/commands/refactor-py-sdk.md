---
allowed-tools: Bash(git commit:*), Bash(git add:*), Bash(git status:*), Bash(mkdir:*), Bash(uv:*), Read, Edit(sdk/python/**), Write(sdk/python/**), Bash(grep:*), Bash(sed:*)
argument-hint: [prompt]
description: Command to refactor Python SDK
---

## Dev rules

- COMMIT your changes in the end with detailed message with the motivation of changes and traces of your actions
- USE `uv` with `--directory sdk/python` command in order to avoid `cd` to the subdirectory
- ALWAYS USE pathes relative to the project root
- DO NOT EVER `cd` into the directories - tool permissions will not be validated properly
- USE ONLY SIMPLE "ls", "grep", "find", "cat" Bash commands and native Claude Code tools - otherwise permission will be blocked
- FORMAT AND TEST code with uv

## Task

$1

## Context

- You must refactor Python SDK with the API similar to the current Typescript SDK located at ../../sdk/typescript
- The Python API should be similar to the Typescript one if possible - so inspect Typescript code before introducing any breaking change in the PUBLIC API
