export type AppRole = 'ADMIN' | 'EDITOR' | 'VIEWER';

const ROLE_LEVELS: Record<AppRole, number> = {
  VIEWER: 1,
  EDITOR: 2,
  ADMIN: 3,
};

export const hasAccess = (userRole: AppRole, requiredRole: AppRole): boolean => {
  return (ROLE_LEVELS[userRole] || 0) >= (ROLE_LEVELS[requiredRole] || 0);
};
