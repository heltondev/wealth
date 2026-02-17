const test = require('node:test');
const assert = require('node:assert/strict');

const {
	inferMaturityDateFromAssetRecord,
	estimateFixedIncomeDuration,
	buildRateSensitivitySnapshot,
	buildMarkToMarketBreakdown,
	sanitizeFixedIncomeShockBps,
} = require('./platform-service');

test('estimateFixedIncomeDuration falls back to zero-coupon proxy when coupon is unavailable', () => {
	const duration = estimateFixedIncomeDuration({
		yearsToMaturity: 5,
		yieldRatePct: 10,
		couponRatePct: 0,
	});

	assert.equal(duration.method, 'zero_coupon_proxy');
	assert.equal(duration.macaulay_duration_years, 5);
	assert.ok(Math.abs(duration.modified_duration_years - (5 / 1.1)) < 1e-9);
	assert.ok(Math.abs(duration.duration_years - duration.modified_duration_years) < 1e-12);
});

test('estimateFixedIncomeDuration computes Macaulay/modified duration for coupon bonds', () => {
	const duration = estimateFixedIncomeDuration({
		yearsToMaturity: 5,
		yieldRatePct: 10,
		couponRatePct: 10,
		couponFrequency: 1,
	});

	assert.equal(duration.method, 'macaulay');
	assert.ok(duration.macaulay_duration_years > 3.5 && duration.macaulay_duration_years < 5);
	assert.ok(duration.modified_duration_years > 3 && duration.modified_duration_years < duration.macaulay_duration_years);
});

test('buildRateSensitivitySnapshot estimates DV01 and +/- rate-shock scenarios', () => {
	const snapshot = buildRateSensitivitySnapshot({
		marketValue: 1000,
		modifiedDurationYears: 4,
		shockBps: 100,
	});

	assert.equal(snapshot.shock_bps, 100);
	assert.ok(Math.abs(snapshot.dv01 - 0.4) < 1e-12);
	assert.ok(Math.abs(snapshot.up_shift_value - 960) < 1e-9);
	assert.ok(Math.abs(snapshot.down_shift_value - 1040) < 1e-9);
	assert.ok(Math.abs(snapshot.up_shift_change + 40) < 1e-9);
	assert.ok(Math.abs(snapshot.down_shift_change - 40) < 1e-9);
});

test('buildMarkToMarketBreakdown splits carry and price effects when carry return is available', () => {
	const breakdown = buildMarkToMarketBreakdown({
		bookValue: 1000,
		marketValue: 1100,
		carryReturnPct: 8,
	});

	assert.equal(breakdown.book_value, 1000);
	assert.equal(breakdown.market_value, 1100);
	assert.equal(breakdown.mark_to_market_value, 100);
	assert.equal(breakdown.carry_value, 80);
	assert.equal(breakdown.price_effect_value, 20);
	assert.equal(breakdown.mark_to_market_return_pct, 10);
	assert.equal(breakdown.price_effect_return_pct, 2);
});

test('inferMaturityDateFromAssetRecord uses explicit maturity fields and ticker fallback', () => {
	assert.equal(
		inferMaturityDateFromAssetRecord({
			maturity: '2031-06-15',
		}),
		'2031-06-15'
	);
	assert.equal(
		inferMaturityDateFromAssetRecord({
			ticker: 'TESOURO-IPCA-2029',
			name: 'Tesouro IPCA+ 2029',
		}),
		'2029-12-31'
	);
	assert.equal(inferMaturityDateFromAssetRecord({ ticker: 'CDB-BANCO-X' }), null);
});

test('sanitizeFixedIncomeShockBps normalizes and bounds rate shock inputs', () => {
	assert.equal(sanitizeFixedIncomeShockBps(-75), 75);
	assert.equal(sanitizeFixedIncomeShockBps(0), 100);
	assert.equal(sanitizeFixedIncomeShockBps(9999), 500);
});
