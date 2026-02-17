const test = require('node:test');
const assert = require('node:assert/strict');

const { filterEventInboxItemsByTrackedTickers } = require('./platform-service');

test('filterEventInboxItemsByTrackedTickers keeps only active tracked tickers', () => {
	const items = [
		{ id: '1', ticker: 'HGLG11', eventDate: '2026-02-17' },
		{ id: '2', ticker: 'ALZR11', eventDate: '2026-02-17' },
		{ id: '3', ticker: 'ABCB4', eventDate: '2026-02-17' },
	];
	const tracked = new Set(['HGLG11', 'ABCB4']);

	const filtered = filterEventInboxItemsByTrackedTickers(items, tracked);
	assert.equal(filtered.length, 2);
	assert.deepEqual(filtered.map((item) => item.id), ['1', '3']);
});

test('filterEventInboxItemsByTrackedTickers normalizes ticker casing', () => {
	const items = [
		{ id: '1', ticker: 'hglg11', eventDate: '2026-02-17' },
		{ id: '2', ticker: 'ALZR11', eventDate: '2026-02-17' },
	];
	const tracked = ['hglg11'];

	const filtered = filterEventInboxItemsByTrackedTickers(items, tracked);
	assert.equal(filtered.length, 1);
	assert.equal(filtered[0].id, '1');
});

test('filterEventInboxItemsByTrackedTickers returns empty list when there are no active tickers', () => {
	const items = [
		{ id: '1', ticker: 'HGLG11', eventDate: '2026-02-17' },
		{ id: '2', ticker: 'ALZR11', eventDate: '2026-02-17' },
	];

	assert.deepEqual(filterEventInboxItemsByTrackedTickers(items, []), []);
	assert.deepEqual(filterEventInboxItemsByTrackedTickers(items, new Set()), []);
});
