import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  type ReactNode,
} from 'react';
import {
  fetchAuthSession,
  getCurrentUser,
  signInWithRedirect,
  signOut,
} from 'aws-amplify/auth';
import { isAmplifyAuthConfigured } from '../aws-exports';
import { resolveRoleFromClaims } from '../utils/authz';
import { logger } from '../utils/logger';

export interface User {
  id: string;
  email: string;
  name: string;
  role: 'ADMIN' | 'EDITOR' | 'VIEWER';
}

interface AuthContextType {
  user: User | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  isAuthConfigured: boolean;
  login: () => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) throw new Error('useAuth must be used within AuthProvider');
  return context;
};

const LOCAL_AUTH_STORAGE_KEY = 'auth_user';

const MOCK_USER: User = {
  id: 'local-user-001',
  email: 'oliver@local.dev',
  name: 'Oliver',
  role: 'ADMIN',
};

const getLocalUser = (): User | null => {
  const saved = localStorage.getItem(LOCAL_AUTH_STORAGE_KEY);
  return saved ? (JSON.parse(saved) as User) : null;
};

const resolveUserFromSession = async (): Promise<User> => {
  const currentUser = await getCurrentUser();
  const session = await fetchAuthSession();
  const claims = (session.tokens?.idToken?.payload || {}) as Record<string, unknown>;

  const id = String(claims.sub || currentUser.userId || currentUser.username);
  const email = String(claims.email || currentUser.signInDetails?.loginId || '').trim().toLowerCase();
  const rawName = String(claims.name || claims.given_name || '').trim();
  const fallbackName = email ? email.split('@')[0] : 'User';

  return {
    id,
    email,
    name: rawName || fallbackName,
    role: resolveRoleFromClaims(claims),
  };
};

export const AuthProvider = ({ children }: { children: ReactNode }) => {
  const [user, setUser] = useState<User | null>(() => {
    if (isAmplifyAuthConfigured) return null;
    return getLocalUser();
  });
  const [isLoading, setIsLoading] = useState<boolean>(isAmplifyAuthConfigured);

  const syncCognitoUser = useCallback(async () => {
    if (!isAmplifyAuthConfigured) {
      setIsLoading(false);
      return;
    }

    setIsLoading(true);
    try {
      const nextUser = await resolveUserFromSession();
      setUser(nextUser);
    } catch {
      setUser(null);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void syncCognitoUser();
  }, [syncCognitoUser]);

  const login = useCallback(async () => {
    if (isAmplifyAuthConfigured) {
      await signInWithRedirect({ provider: 'Google' });
      return;
    }

    setUser(MOCK_USER);
    localStorage.setItem(LOCAL_AUTH_STORAGE_KEY, JSON.stringify(MOCK_USER));
  }, []);

  const logout = useCallback(() => {
    setUser(null);
    if (isAmplifyAuthConfigured) {
      void signOut().catch((error) => {
        logger.error('Error signing out', error);
      });
      return;
    }
    localStorage.removeItem(LOCAL_AUTH_STORAGE_KEY);
  }, []);

  return (
    <AuthContext.Provider
      value={{
        user,
        isAuthenticated: !!user,
        isLoading,
        isAuthConfigured: isAmplifyAuthConfigured,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};
