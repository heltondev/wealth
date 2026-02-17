const DEFAULT_PERIOD = '12';
const DEFAULT_METHOD = 'fifo';

const toOption = (option) => {
  const rawValue = String(option?.value || '').trim();
  const rawLabel = String(option?.label || option?.value || '').trim();
  return {
    value: rawValue,
    label: rawLabel || rawValue,
  };
};

export const normalizeNumericOptions = (options, fallbackValue = DEFAULT_PERIOD) => {
  const normalized = [];
  const seen = new Set();

  for (const option of Array.isArray(options) ? options : []) {
    const current = toOption(option);
    const parsed = Number(current.value);
    if (!current.value || !Number.isFinite(parsed) || parsed <= 0) continue;
    if (seen.has(current.value)) continue;
    seen.add(current.value);
    normalized.push(current);
  }

  if (normalized.length > 0) {
    return normalized;
  }

  return [{ value: fallbackValue, label: fallbackValue }];
};

export const normalizeMethodOptions = (options, fallbackValue = DEFAULT_METHOD) => {
  const allowed = new Set(['fifo', 'weighted_average']);
  const normalized = [];
  const seen = new Set();

  for (const option of Array.isArray(options) ? options : []) {
    const current = toOption(option);
    const value = current.value.toLowerCase();
    if (!value || !allowed.has(value) || seen.has(value)) continue;
    seen.add(value);
    normalized.push({
      value,
      label: current.label,
    });
  }

  if (normalized.length > 0) {
    return normalized;
  }

  return [{ value: fallbackValue, label: fallbackValue.toUpperCase() }];
};

export const resolveSelectableValue = (currentValue, options, fallbackValue) => {
  const normalizedCurrent = String(currentValue || '').trim();
  const availableValues = (Array.isArray(options) ? options : [])
    .map((option) => String(option?.value || '').trim())
    .filter(Boolean);

  if (availableValues.includes(normalizedCurrent)) {
    return normalizedCurrent;
  }

  if (availableValues.length > 0) {
    return availableValues[0];
  }

  return String(fallbackValue || '').trim();
};

export const resolveCalendarMonthValue = (currentMonth, availableMonths, todayMonth) => {
  const normalizedCurrent = String(currentMonth || '').trim();
  const normalizedToday = String(todayMonth || '').trim();
  const months = (Array.isArray(availableMonths) ? availableMonths : [])
    .map((month) => String(month || '').trim())
    .filter(Boolean);

  if (months.includes(normalizedCurrent)) {
    return normalizedCurrent;
  }

  if (normalizedToday && months.includes(normalizedToday)) {
    return normalizedToday;
  }

  if (months.length > 0) {
    return months[months.length - 1];
  }

  return normalizedToday;
};
