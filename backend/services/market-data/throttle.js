const { sleep } = require('./utils');

/**
 * Creates a scheduler that limits max concurrent executions and enforces
 * a minimum delay between task starts to reduce provider blocking risk.
 */
function createThrottledScheduler(options = {}) {
	const minDelayMs = Math.max(0, Number(options.minDelayMs || 250));
	const maxConcurrent = Math.max(1, Number(options.maxConcurrent || 2));

	let activeCount = 0;
	let lastStartAt = 0;
	const queue = [];

	const runNext = () => {
		if (activeCount >= maxConcurrent) return;
		const next = queue.shift();
		if (!next) return;

		activeCount += 1;

		const execute = async () => {
			try {
				const waitMs = Math.max(0, minDelayMs - (Date.now() - lastStartAt));
				if (waitMs > 0) await sleep(waitMs);
				lastStartAt = Date.now();
				const result = await next.task();
				next.resolve(result);
			} catch (error) {
				next.reject(error);
			} finally {
				activeCount -= 1;
				runNext();
			}
		};

		execute();
	};

	return (task) =>
		new Promise((resolve, reject) => {
			queue.push({ task, resolve, reject });
			runNext();
		});
}

module.exports = {
	createThrottledScheduler,
};
