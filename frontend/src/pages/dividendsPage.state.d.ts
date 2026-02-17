export type DropdownOptionLike = {
  value: string;
  label: string;
};

export function normalizeNumericOptions(
  options: unknown,
  fallbackValue?: string
): DropdownOptionLike[];

export function normalizeMethodOptions(
  options: unknown,
  fallbackValue?: string
): DropdownOptionLike[];

export function resolveSelectableValue(
  currentValue: unknown,
  options: Array<{ value: unknown }>,
  fallbackValue: unknown
): string;

export function resolveCalendarMonthValue(
  currentMonth: unknown,
  availableMonths: unknown,
  todayMonth: unknown
): string;
