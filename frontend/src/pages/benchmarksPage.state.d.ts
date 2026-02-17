export type BenchmarkSelectionState = {
  selectedBenchmarks: string[];
  selectionInitialized: boolean;
};

export type BenchmarkSelectionReconcileResult = BenchmarkSelectionState & {
  changed: boolean;
};

export function normalizeBenchmarkCode(value: unknown): string;

export function resolveDefaultBenchmarkSelection(
  availableBenchmarkCodes: unknown,
  defaultCodes?: unknown
): string[];

export function reconcileBenchmarkSelectionState(
  previousState: BenchmarkSelectionState,
  availableBenchmarkCodes: unknown,
  defaultCodes?: unknown
): BenchmarkSelectionReconcileResult;
