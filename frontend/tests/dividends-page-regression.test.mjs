import assert from 'node:assert/strict';
import test from 'node:test';
import {
  normalizeMethodOptions,
  normalizeNumericOptions,
  resolveCalendarMonthValue,
  resolveSelectableValue,
} from '../src/pages/dividendsPage.state.js';

test('normalizeNumericOptions keeps only positive numeric values and deduplicates', () => {
  const options = normalizeNumericOptions([
    { value: '12', label: '12M' },
    { value: '0', label: 'invalid' },
    { value: '12', label: 'duplicate' },
    { value: '24', label: '24M' },
  ]);

  assert.deepEqual(options, [
    { value: '12', label: '12M' },
    { value: '24', label: '24M' },
  ]);
});

test('normalizeMethodOptions enforces allowed calculation methods', () => {
  const options = normalizeMethodOptions([
    { value: 'FIFO', label: 'FIFO' },
    { value: 'weighted_average', label: 'MÉDIA' },
    { value: 'invalid', label: 'INVALID' },
  ]);

  assert.deepEqual(options, [
    { value: 'fifo', label: 'FIFO' },
    { value: 'weighted_average', label: 'MÉDIA' },
  ]);
});

test('resolveSelectableValue is idempotent for valid current values', () => {
  const options = [
    { value: '6', label: '6M' },
    { value: '12', label: '12M' },
  ];

  const resolved = resolveSelectableValue('12', options, '12');
  assert.equal(resolved, '12');

  const stabilized = resolveSelectableValue(resolved, options, '12');
  assert.equal(stabilized, '12');
});

test('resolveSelectableValue falls back deterministically when current is invalid', () => {
  const options = [
    { value: '3', label: '3M' },
    { value: '6', label: '6M' },
  ];

  assert.equal(resolveSelectableValue('999', options, '12'), '3');
  assert.equal(resolveSelectableValue('999', [], '12'), '12');
});

test('resolveCalendarMonthValue keeps calendar selection stable', () => {
  const months = ['2026-01', '2026-02', '2026-03'];

  assert.equal(resolveCalendarMonthValue('2026-02', months, '2026-02'), '2026-02');
  assert.equal(resolveCalendarMonthValue('2025-12', months, '2026-02'), '2026-02');
  assert.equal(resolveCalendarMonthValue('2025-12', months, '2025-12'), '2026-03');
  assert.equal(resolveCalendarMonthValue('', [], '2026-02'), '2026-02');
});
