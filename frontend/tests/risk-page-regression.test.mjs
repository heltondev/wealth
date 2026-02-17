import assert from 'node:assert/strict';
import test from 'node:test';
import {
  classifyReturnValue,
  filterScatterRowsByReturn,
  resolveRiskThresholdValue,
} from '../src/pages/riskPage.state.js';

test('resolveRiskThresholdValue returns deterministic positive threshold', () => {
  assert.equal(resolveRiskThresholdValue('20', 15), 20);
  assert.equal(resolveRiskThresholdValue('0', 15), 15);
  assert.equal(resolveRiskThresholdValue('invalid', 15), 15);
  assert.equal(resolveRiskThresholdValue('invalid', 'invalid'), 15);
});

test('classifyReturnValue uses epsilon guard around zero', () => {
  assert.equal(classifyReturnValue(0), 'neutral');
  assert.equal(classifyReturnValue(0.01), 'positive');
  assert.equal(classifyReturnValue(-0.01), 'negative');
  assert.equal(classifyReturnValue(0.0001), 'neutral');
});

test('filterScatterRowsByReturn filters consistently by return class', () => {
  const rows = [
    { ticker: 'AAA', returnPct: 1.2 },
    { ticker: 'BBB', returnPct: -0.8 },
    { ticker: 'CCC', returnPct: 0 },
  ];

  const allRows = filterScatterRowsByReturn(rows, 'all');
  assert.strictEqual(allRows, rows);

  const positive = filterScatterRowsByReturn(rows, 'positive').map((row) => row.ticker);
  const negative = filterScatterRowsByReturn(rows, 'negative').map((row) => row.ticker);
  const neutral = filterScatterRowsByReturn(rows, 'neutral').map((row) => row.ticker);

  assert.deepEqual(positive, ['AAA']);
  assert.deepEqual(negative, ['BBB']);
  assert.deepEqual(neutral, ['CCC']);
});
