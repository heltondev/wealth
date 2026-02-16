const test = require('node:test');
const assert = require('node:assert/strict');

const { extractFirstNumber } = require('./extractors');

test('extractFirstNumber parses pt-BR number formats', () => {
	assert.equal(extractFirstNumber('R$ 1.234,56'), 1234.56);
	assert.equal(extractFirstNumber('49.500,93'), 49500.93);
});

test('extractFirstNumber parses en-US number formats', () => {
	assert.equal(extractFirstNumber('$1,234.56'), 1234.56);
	assert.equal(extractFirstNumber('49,500.93'), 49500.93);
});

test('extractFirstNumber preserves decimal percentages', () => {
	assert.equal(extractFirstNumber('0.65%'), 0.65);
	assert.equal(extractFirstNumber('-2.23%'), -2.23);
});
