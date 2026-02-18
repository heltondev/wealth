export type AppRole = 'ADMIN' | 'EDITOR' | 'VIEWER';

const ROLE_LEVELS: Record<AppRole, number> = {
  VIEWER: 1,
  EDITOR: 2,
  ADMIN: 3,
};

const normalizeRole = (value: unknown): AppRole => {
  if (typeof value !== 'string') return 'VIEWER';
  const upper = value.toUpperCase();
  if (upper === 'ADMIN' || upper === 'EDITOR' || upper === 'VIEWER') {
    return upper;
  }
  return 'VIEWER';
};

const parseGroups = (groupsClaim: unknown): string[] => {
  if (Array.isArray(groupsClaim)) {
    return groupsClaim
      .map((group) => String(group || '').trim().toUpperCase())
      .filter(Boolean);
  }

  if (typeof groupsClaim === 'string') {
    return groupsClaim
      .split(',')
      .map((group) => group.replace(/\[|\]|"/g, '').trim().toUpperCase())
      .filter(Boolean);
  }

  return [];
};

export const resolveRoleFromClaims = (claims: Record<string, unknown>): AppRole => {
  const groups = parseGroups(claims['cognito:groups']);
  if (groups.includes('ADMIN')) return 'ADMIN';
  if (groups.includes('EDITOR')) return 'EDITOR';
  if (groups.includes('VIEWER')) return 'VIEWER';
  // Backend defaults to ADMIN when no group is assigned in v1.
  return normalizeRole('ADMIN');
};

export const hasAccess = (userRole: AppRole, requiredRole: AppRole): boolean => {
  return (ROLE_LEVELS[userRole] || 0) >= (ROLE_LEVELS[requiredRole] || 0);
};
