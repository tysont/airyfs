// ABOUTME: Compares two deployed benchmark reports using their median scenario results.
// ABOUTME: Reports latency speedups and Hrana amplification changes for optimization experiments.

import { readFile } from 'node:fs/promises';
import { compareSummaries, scoreReport, validateComparableReports } from './benchmark-lib.mjs';

const [baselinePath, candidatePath] = process.argv.slice(2);
if (!baselinePath || !candidatePath) {
  throw new Error('usage: npm run benchmark:compare -- BASELINE.json CANDIDATE.json');
}
const baseline = JSON.parse(await readFile(baselinePath, 'utf8'));
const candidate = JSON.parse(await readFile(candidatePath, 'utf8'));
validateComparableReports(baseline, candidate);
const comparison = compareSummaries(baseline.summary, candidate.summary);
if (comparison.length === 0) throw new Error('Reports have no matching scenarios');
const score = scoreReport(baseline, candidate);

console.log(`Baseline:  ${baseline.label || baseline.harnessRevision || baselinePath}`);
console.log(`Candidate: ${candidate.label || candidate.harnessRevision || candidatePath}`);
console.log(`Overall performance score: ${score.overall}`);
console.table(Object.entries(score.groups).map(([group, value]) => ({ group, score: value })));
console.table(comparison.map((entry) => ({
  chunk: entry.chunkSize,
  scenario: entry.name,
  baselineMs: round(entry.baselineMs),
  candidateMs: round(entry.candidateMs),
  speedup: entry.speedup === null ? null : `${round(entry.speedup)}x`,
  latencyDelta: percent(entry.operationMsChangePercent),
  pipelineDelta: percent(entry.pipelineRequestsChangePercent),
  statementDelta: percent(entry.sqlStatementsChangePercent),
})));

function round(value) {
  return value === null ? null : Math.round(value * 100) / 100;
}

function percent(value) {
  return value === null ? null : `${value >= 0 ? '+' : ''}${round(value)}%`;
}
