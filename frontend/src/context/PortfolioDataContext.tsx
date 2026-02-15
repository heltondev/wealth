import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import { api, type Asset, type Portfolio, type Transaction } from '../services/api';

interface PortfolioMetricsData {
  marketValues: Record<string, number | null>;
  averageCosts: Record<string, number | null>;
  currentQuotes: Record<string, number | null>;
  fetchedAt: number;
}

interface PortfolioDataContextType {
  portfolios: Portfolio[];
  selectedPortfolio: string;
  setSelectedPortfolio: (id: string) => void;
  assets: Asset[];
  transactions: Transaction[];
  loading: boolean;
  metrics: PortfolioMetricsData | null;
  refreshMetrics: () => void;
}

const METRICS_TTL_MS = 5 * 60 * 1000;
const METRICS_STORAGE_VERSION = 'v2';

const storageKeyMetrics = (portfolioId: string) =>
  `portfolio_metrics_${METRICS_STORAGE_VERSION}_${portfolioId}`;
const STORAGE_KEY_SELECTED = 'portfolio_selected';

const readMetricsFromStorage = (portfolioId: string): PortfolioMetricsData | null => {
  if (!portfolioId) return null;
  try {
    const raw = localStorage.getItem(storageKeyMetrics(portfolioId));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as PortfolioMetricsData;
    if (parsed && typeof parsed.fetchedAt === 'number') return parsed;
  } catch {
    // Ignore corrupt storage entries.
  }
  return null;
};

const writeMetricsToStorage = (portfolioId: string, data: PortfolioMetricsData) => {
  if (!portfolioId) return;
  try {
    localStorage.setItem(storageKeyMetrics(portfolioId), JSON.stringify(data));
  } catch {
    // Quota exceeded or private browsing; safe to ignore.
  }
};

const readSelectedFromStorage = (): string => {
  try {
    return localStorage.getItem(STORAGE_KEY_SELECTED) || '';
  } catch {
    return '';
  }
};

const writeSelectedToStorage = (id: string) => {
  try {
    localStorage.setItem(STORAGE_KEY_SELECTED, id);
  } catch {
    // Ignore.
  }
};

const parseOptionalNumber = (value: unknown): number | null => {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const numeric = Number(trimmed);
    return Number.isFinite(numeric) ? numeric : null;
  }

  return null;
};

const parseMetricsPayload = (payload: unknown): Omit<PortfolioMetricsData, 'fetchedAt'> => {
  const metrics = Array.isArray((payload as { assets?: unknown[] }).assets)
    ? (payload as { assets: unknown[] }).assets
    : [];

  const marketValues: Record<string, number | null> = {};
  const averageCosts: Record<string, number | null> = {};
  const currentQuotes: Record<string, number | null> = {};

  for (const item of metrics) {
    const metric = item as Record<string, unknown>;
    const assetId = String(metric.assetId || '');
    if (!assetId) continue;

    const marketValue = parseOptionalNumber(metric.market_value);
    const averageCost = parseOptionalNumber(metric.average_cost);
    const currentPrice = parseOptionalNumber(metric.current_price);
    const quantityCurrent = parseOptionalNumber(metric.quantity_current);
    const resolvedMarketValue =
      marketValue !== null
        ? marketValue
        : (currentPrice !== null && quantityCurrent !== null)
          ? currentPrice * quantityCurrent
          : null;

    if (resolvedMarketValue !== null) {
      marketValues[assetId] = resolvedMarketValue;
    }
    if (averageCost !== null) {
      averageCosts[assetId] = averageCost;
    }
    if (currentPrice !== null) {
      currentQuotes[assetId] = currentPrice;
    }
  }

  return { marketValues, averageCosts, currentQuotes };
};

const PortfolioDataContext = createContext<PortfolioDataContextType | undefined>(undefined);

export const usePortfolioData = () => {
  const context = useContext(PortfolioDataContext);
  if (!context) throw new Error('usePortfolioData must be used within PortfolioDataProvider');
  return context;
};

export const PortfolioDataProvider = ({ children }: { children: ReactNode }) => {
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedPortfolio, setSelectedPortfolioRaw] = useState<string>(readSelectedFromStorage);
  const [assets, setAssets] = useState<Asset[]>([]);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  const [metrics, setMetrics] = useState<PortfolioMetricsData | null>(() =>
    readMetricsFromStorage(readSelectedFromStorage())
  );

  const setSelectedPortfolio = useCallback((id: string) => {
    setSelectedPortfolioRaw(id);
    writeSelectedToStorage(id);
    const cached = readMetricsFromStorage(id);
    setMetrics(cached);
  }, []);

  // Load portfolios on mount.
  useEffect(() => {
    api.getPortfolios()
      .then((items) => {
        setPortfolios(items);
        if (items.length > 0) {
          setSelectedPortfolioRaw((prev) => {
            if (prev && items.some((p) => p.portfolioId === prev)) return prev;
            const fallback = items[0].portfolioId;
            writeSelectedToStorage(fallback);
            const cached = readMetricsFromStorage(fallback);
            setMetrics(cached);
            return fallback;
          });
        }
      })
      .catch(() => setPortfolios([]))
      .finally(() => setLoading(false));
  }, []);

  // Load assets + transactions when portfolio changes.
  useEffect(() => {
    if (!selectedPortfolio) return;
    let cancelled = false;
    setLoading(true);

    Promise.all([api.getAssets(selectedPortfolio), api.getTransactions(selectedPortfolio)])
      .then(([assetItems, transactionItems]) => {
        if (cancelled) return;
        setAssets(assetItems);
        setTransactions(transactionItems);
      })
      .catch(() => {
        if (cancelled) return;
        setAssets([]);
        setTransactions([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [selectedPortfolio]);

  // Fetch metrics with TTL.
  const fetchMetrics = useCallback((portfolioId: string) => {
    if (!portfolioId) return;

    api.getPortfolioMetrics(portfolioId)
      .then((payload) => {
        const parsed = parseMetricsPayload(payload);
        const data: PortfolioMetricsData = { ...parsed, fetchedAt: Date.now() };
        writeMetricsToStorage(portfolioId, data);
        setSelectedPortfolioRaw((current) => {
          if (current === portfolioId) setMetrics(data);
          return current;
        });
      })
      .catch(() => {
        // Keep cached values on transient errors.
      });
  }, []);

  useEffect(() => {
    if (!selectedPortfolio) return;
    const cached = readMetricsFromStorage(selectedPortfolio);
    if (cached && (Date.now() - cached.fetchedAt) < METRICS_TTL_MS) {
      setMetrics(cached);
      return;
    }
    fetchMetrics(selectedPortfolio);
  }, [selectedPortfolio, assets.length, transactions.length, fetchMetrics]);

  const refreshMetrics = useCallback(() => {
    if (selectedPortfolio) fetchMetrics(selectedPortfolio);
  }, [fetchMetrics, selectedPortfolio]);

  const value = useMemo<PortfolioDataContextType>(() => ({
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    assets,
    transactions,
    loading,
    metrics,
    refreshMetrics,
  }), [portfolios, selectedPortfolio, setSelectedPortfolio, assets, transactions, loading, metrics, refreshMetrics]);

  return (
    <PortfolioDataContext.Provider value={value}>
      {children}
    </PortfolioDataContext.Provider>
  );
};
