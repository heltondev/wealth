const DEFAULT_SELECTED_BENCHMARKS = ['IBOV', 'IFIX', 'CDI', 'IPCA', 'SELIC', 'POUPANCA'];

export const normalizeBenchmarkCode = (value) => String(value || '').toUpperCase().trim();

const toUniqueNormalizedList = (values) => {
  const seen = new Set();
  const normalized = [];

  for (const value of Array.isArray(values) ? values : []) {
    const code = normalizeBenchmarkCode(value);
    if (!code || seen.has(code)) continue;
    seen.add(code);
    normalized.push(code);
  }

  return normalized;
};

const areSameCodeList = (left, right) => {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
};

export const resolveDefaultBenchmarkSelection = (availableBenchmarkCodes, defaultCodes = DEFAULT_SELECTED_BENCHMARKS) => {
  const available = toUniqueNormalizedList(availableBenchmarkCodes);
  if (available.length === 0) return [];

  const defaults = toUniqueNormalizedList(defaultCodes)
    .filter((code) => available.includes(code));

  if (defaults.length > 0) {
    return defaults;
  }

  return [available[0]];
};

export const reconcileBenchmarkSelectionState = (previousState, availableBenchmarkCodes, defaultCodes = DEFAULT_SELECTED_BENCHMARKS) => {
  const available = toUniqueNormalizedList(availableBenchmarkCodes);
  const previousSelectedRaw = Array.isArray(previousState?.selectedBenchmarks)
    ? previousState.selectedBenchmarks
    : [];
  const previousInitialized = Boolean(previousState?.selectionInitialized);
  const previousSelectedRawNormalized = toUniqueNormalizedList(previousSelectedRaw);

  const previousSelected = previousSelectedRawNormalized
    .filter((code) => available.includes(code));

  if (available.length === 0) {
    const changed = previousInitialized || previousSelectedRaw.length > 0;
    return {
      selectedBenchmarks: [],
      selectionInitialized: false,
      changed,
    };
  }

  const nextSelected = previousInitialized
    ? previousSelected
    : resolveDefaultBenchmarkSelection(available, defaultCodes);

  const changed = !areSameCodeList(previousSelectedRawNormalized, nextSelected) || !previousInitialized;

  return {
    selectedBenchmarks: nextSelected,
    selectionInitialized: true,
    changed,
  };
};
