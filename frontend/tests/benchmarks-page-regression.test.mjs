import assert from 'node:assert/strict';
import test from 'node:test';
import {
  reconcileBenchmarkSelectionState,
  resolveDefaultBenchmarkSelection,
} from '../src/pages/benchmarksPage.state.js';

test('resolveDefaultBenchmarkSelection prefers configured defaults in order', () => {
  const selection = resolveDefaultBenchmarkSelection(['CDI', 'IFIX', 'IBOV', 'SNP500']);
  assert.deepEqual(selection, ['IBOV', 'IFIX', 'CDI']);
});

test('reconcileBenchmarkSelectionState initializes once and then stabilizes', () => {
  const initial = {
    selectedBenchmarks: [],
    selectionInitialized: false,
  };

  const firstPass = reconcileBenchmarkSelectionState(initial, ['IBOV', 'IFIX', 'CDI']);
  assert.equal(firstPass.changed, true);
  assert.equal(firstPass.selectionInitialized, true);
  assert.deepEqual(firstPass.selectedBenchmarks, ['IBOV', 'IFIX', 'CDI']);

  const secondPass = reconcileBenchmarkSelectionState(firstPass, ['IBOV', 'IFIX', 'CDI']);
  assert.equal(secondPass.changed, false);
  assert.deepEqual(secondPass.selectedBenchmarks, firstPass.selectedBenchmarks);
});

test('reconcileBenchmarkSelectionState removes stale codes when available set changes', () => {
  const previous = {
    selectedBenchmarks: ['IBOV', 'IFIX', 'SNP500'],
    selectionInitialized: true,
  };

  const next = reconcileBenchmarkSelectionState(previous, ['IBOV', 'IFIX']);
  assert.equal(next.changed, true);
  assert.deepEqual(next.selectedBenchmarks, ['IBOV', 'IFIX']);
  assert.equal(next.selectionInitialized, true);

  const stabilized = reconcileBenchmarkSelectionState(next, ['IBOV', 'IFIX']);
  assert.equal(stabilized.changed, false);
});

test('reconcileBenchmarkSelectionState resets cleanly when list becomes empty', () => {
  const previous = {
    selectedBenchmarks: ['IBOV'],
    selectionInitialized: true,
  };

  const next = reconcileBenchmarkSelectionState(previous, []);
  assert.equal(next.changed, true);
  assert.deepEqual(next.selectedBenchmarks, []);
  assert.equal(next.selectionInitialized, false);
});
