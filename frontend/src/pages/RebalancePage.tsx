import { useCallback, useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import Layout from '../components/Layout';
import SharedDropdown from '../components/SharedDropdown';
import { usePortfolioData } from '../context/PortfolioDataContext';
import {
  api,
  type RebalanceDriftItem,
  type RebalanceSuggestionItem,
  type RebalanceSuggestionResponse,
  type RebalanceTarget,
  type RebalanceThesisConflict,
} from '../services/api';
import { formatCurrency } from '../utils/formatters';
import './RebalancePage.scss';

type RebalanceScope = 'assetClass' | 'asset';

interface EditableTargetRow {
  localId: string;
  targetId?: string;
  scope: RebalanceScope;
  value: string;
  percent: string;
}

const DEFAULT_CONTRIBUTION_BRL = '1000';
const DEFAULT_CONTRIBUTION_USD = '0';

const normalizeScope = (value: unknown): RebalanceScope => {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === 'asset' ? 'asset' : 'assetClass';
};

const toNumber = (value: unknown): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

const toTitleCase = (value: string): string =>
  String(value || '')
    .replace(/_/g, ' ')
    .split(' ')
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(' ');

const newLocalId = (): string =>
  `target-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;

const createEditableRow = (
  scope: RebalanceScope,
  value = '',
  percent = '',
  targetId?: string
): EditableTargetRow => ({
  localId: newLocalId(),
  targetId,
  scope,
  value,
  percent,
});

const toPercentText = (value: unknown): string => {
  const numeric = toNumber(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return '';
  return numeric.toLocaleString('en-US', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 4,
  });
};

const splitTargetsByScope = (targets: RebalanceTarget[]) => {
  const byScope: Record<RebalanceScope, EditableTargetRow[]> = {
    assetClass: [],
    asset: [],
  };

  for (const target of targets || []) {
    const scope = normalizeScope(target.scope);
    byScope[scope].push(
      createEditableRow(
        scope,
        String(target.value || ''),
        toPercentText(target.percent),
        target.targetId
      )
    );
  }

  byScope.assetClass.sort((left, right) => left.value.localeCompare(right.value));
  byScope.asset.sort((left, right) => left.value.localeCompare(right.value));
  return byScope;
};

const RebalancePage = () => {
  const { t, i18n } = useTranslation();
  const { portfolios, selectedPortfolio, setSelectedPortfolio, assets } = usePortfolioData();
  const [scope, setScope] = useState<RebalanceScope>('assetClass');
  const [contributionAmountBrl, setContributionAmountBrl] = useState(DEFAULT_CONTRIBUTION_BRL);
  const [contributionAmountUsd, setContributionAmountUsd] = useState(DEFAULT_CONTRIBUTION_USD);
  const [targetsByScope, setTargetsByScope] = useState<Record<RebalanceScope, EditableTargetRow[]>>({
    assetClass: [],
    asset: [],
  });
  const [suggestion, setSuggestion] = useState<RebalanceSuggestionResponse | null>(null);
  const [loadingTargets, setLoadingTargets] = useState(false);
  const [loadingSuggestion, setLoadingSuggestion] = useState(false);
  const [savingTargets, setSavingTargets] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';
  const formatBrl = (value: number) => formatCurrency(value, 'BRL', numberLocale);
  const formatSignedBrl = (value: number) => {
    const amount = formatBrl(Math.abs(value));
    if (value > 0) return `+${amount}`;
    if (value < 0) return `-${amount}`;
    return amount;
  };
  const formatPercent = (value: number | null | undefined, fractionDigits = 2) => {
    if (typeof value !== 'number' || !Number.isFinite(value)) return '-';
    return `${value.toLocaleString(numberLocale, {
      minimumFractionDigits: fractionDigits,
      maximumFractionDigits: fractionDigits,
    })}%`;
  };

  const portfolioOptions = useMemo(
    () => portfolios.map((portfolio) => ({ value: portfolio.portfolioId, label: portfolio.name })),
    [portfolios]
  );

  const scopeOptions = useMemo(
    () => [
      { value: 'assetClass', label: t('rebalance.scopes.assetClass') },
      { value: 'asset', label: t('rebalance.scopes.asset') },
    ],
    [t]
  );

  const classOptions = useMemo(() => {
    const fromAssets = assets
      .map((asset) => String(asset.assetClass || '').toLowerCase())
      .filter(Boolean);
    const fallback = ['stock', 'fii', 'bond', 'crypto', 'rsu', 'derivative'];
    const values = Array.from(new Set([...fromAssets, ...fallback])).sort((left, right) =>
      left.localeCompare(right)
    );
    return values.map((value) => ({
      value,
      label: t(`assets.classes.${value}`, { defaultValue: toTitleCase(value) }),
    }));
  }, [assets, t]);

  const assetOptions = useMemo(() => {
    const sorted = [...assets].sort((left, right) => {
      const leftActive = String(left.status || '').toLowerCase() === 'active' ? 0 : 1;
      const rightActive = String(right.status || '').toLowerCase() === 'active' ? 0 : 1;
      if (leftActive !== rightActive) return leftActive - rightActive;
      return String(left.ticker || '').localeCompare(String(right.ticker || ''));
    });
    return sorted.map((asset) => ({
      value: asset.assetId,
      label: `${asset.ticker} • ${asset.name}`,
    }));
  }, [assets]);

  const assetById = useMemo(
    () =>
      assets.reduce<Record<string, (typeof assets)[number]>>((accumulator, asset) => {
        accumulator[asset.assetId] = asset;
        return accumulator;
      }, {}),
    [assets]
  );

  const targetOptionsByScope = useMemo(
    () => ({
      assetClass: classOptions,
      asset: assetOptions,
    }),
    [assetOptions, classOptions]
  );

  const loadTargets = useCallback((portfolioId: string) => {
    if (!portfolioId) return Promise.resolve();
    setLoadingTargets(true);
    return api.getRebalanceTargets(portfolioId)
      .then((response) => {
        setTargetsByScope(splitTargetsByScope(response.targets));
      })
      .catch((reason: unknown) => {
        setTargetsByScope({ assetClass: [], asset: [] });
        const message = reason instanceof Error ? reason.message : 'Failed to load targets';
        setError(message);
      })
      .finally(() => setLoadingTargets(false));
  }, []);

  const runSuggestion = useCallback(() => {
    if (!selectedPortfolio) return;
    const amountBrl = Math.max(0, toNumber(contributionAmountBrl));
    const amountUsd = Math.max(0, toNumber(contributionAmountUsd));
    if (amountBrl <= 0 && amountUsd <= 0) {
      setError(t('rebalance.messages.invalidAmount'));
      return;
    }
    const totalAmount = amountBrl + amountUsd;

    setLoadingSuggestion(true);
    setError(null);

    api.getRebalanceSuggestion(selectedPortfolio, totalAmount, scope, {
      amountBrl,
      amountUsd,
    })
      .then((response) => {
        setSuggestion(response);
      })
      .catch((reason: unknown) => {
        const message = reason instanceof Error ? reason.message : 'Failed to load suggestion';
        setError(message);
        setSuggestion(null);
      })
      .finally(() => setLoadingSuggestion(false));
  }, [contributionAmountBrl, contributionAmountUsd, scope, selectedPortfolio, t]);

  useEffect(() => {
    if (!selectedPortfolio) {
      setTargetsByScope({ assetClass: [], asset: [] });
      setSuggestion(null);
      return;
    }

    setError(null);
    setNotice(null);

    loadTargets(selectedPortfolio)
      .catch(() => {
        // Error state already handled above.
      });
  }, [loadTargets, selectedPortfolio]);

  useEffect(() => {
    setNotice(null);
    setSuggestion(null);
  }, [scope, selectedPortfolio]);

  const currentRows = targetsByScope[scope];
  const currentOptions = targetOptionsByScope[scope];

  const percentSum = useMemo(
    () => currentRows.reduce((sum, row) => sum + toNumber(row.percent), 0),
    [currentRows]
  );

  const valueDuplicates = useMemo(() => {
    const counts = new Map<string, number>();
    for (const row of currentRows) {
      const key = String(row.value || '').trim().toLowerCase();
      if (!key) continue;
      counts.set(key, (counts.get(key) || 0) + 1);
    }
    return new Set(
      Array.from(counts.entries())
        .filter(([, count]) => count > 1)
        .map(([key]) => key)
    );
  }, [currentRows]);

  const currentScopeHasInvalidRows = useMemo(
    () => currentRows.some((row) => {
      const value = String(row.value || '').trim().toLowerCase();
      const percent = toNumber(row.percent);
      if (!value || percent <= 0) return true;
      return valueDuplicates.has(value);
    }),
    [currentRows, valueDuplicates]
  );

  const serializedTargets = useMemo<RebalanceTarget[]>(() => {
    const rows = [...targetsByScope.assetClass, ...targetsByScope.asset];
    const result: RebalanceTarget[] = [];
    const seen = new Set<string>();

    for (const row of rows) {
      const value = String(row.value || '').trim();
      const percent = toNumber(row.percent);
      if (!value || percent <= 0) continue;
      const dedupeKey = `${row.scope}:${value.toLowerCase()}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      result.push({
        targetId: row.targetId,
        scope: row.scope,
        value,
        percent,
      });
    }

    return result;
  }, [targetsByScope]);

  const hasAnyInvalidRows = useMemo(() => {
    const byScope: RebalanceScope[] = ['assetClass', 'asset'];
    for (const scopeKey of byScope) {
      const rows = targetsByScope[scopeKey];
      const seen = new Set<string>();
      for (const row of rows) {
        const value = String(row.value || '').trim().toLowerCase();
        const percent = toNumber(row.percent);
        if (!value || percent <= 0) return true;
        const key = `${scopeKey}:${value}`;
        if (seen.has(key)) return true;
        seen.add(key);
      }
    }
    return false;
  }, [targetsByScope]);

  const handleAddTarget = () => {
    const fallbackValue = currentOptions[0]?.value || '';
    setTargetsByScope((prev) => ({
      ...prev,
      [scope]: [
        ...prev[scope],
        createEditableRow(scope, fallbackValue, ''),
      ],
    }));
  };

  const handleRemoveTarget = (localId: string) => {
    setTargetsByScope((prev) => ({
      ...prev,
      [scope]: prev[scope].filter((row) => row.localId !== localId),
    }));
  };

  const handleTargetChange = (localId: string, patch: Partial<EditableTargetRow>) => {
    setTargetsByScope((prev) => ({
      ...prev,
      [scope]: prev[scope].map((row) => (
        row.localId === localId
          ? { ...row, ...patch }
          : row
      )),
    }));
  };

  const handleSaveTargets = () => {
    if (!selectedPortfolio) return;
    if (hasAnyInvalidRows) {
      setError(t('rebalance.messages.invalidTargets'));
      return;
    }

    setSavingTargets(true);
    setError(null);
    setNotice(null);

    api.setRebalanceTargets(selectedPortfolio, serializedTargets)
      .then((response) => {
        setTargetsByScope(splitTargetsByScope(response.targets));
        setNotice(t('rebalance.messages.saved'));
        runSuggestion();
      })
      .catch((reason: unknown) => {
        const message = reason instanceof Error ? reason.message : 'Failed to save rebalance targets';
        setError(message);
      })
      .finally(() => setSavingTargets(false));
  };

  const driftRows = useMemo(
    () => [...(suggestion?.drift || [])].sort(
      (left, right) => Math.abs(toNumber(right.drift_value)) - Math.abs(toNumber(left.drift_value))
    ),
    [suggestion?.drift]
  );

  const suggestionRows = useMemo(
    () => [...(suggestion?.suggestions || [])].sort(
      (left, right) => toNumber(right.recommended_amount) - toNumber(left.recommended_amount)
    ),
    [suggestion?.suggestions]
  );

  const thesisDiagnostics = suggestion?.thesis_diagnostics || null;
  const targetSource = String(suggestion?.target_source || 'equal_weight');

  const thesisConflictRows = useMemo(
    () => [...(thesisDiagnostics?.conflicts || [])].sort(
      (left, right) => Math.abs(toNumber(right.actual_pct)) - Math.abs(toNumber(left.actual_pct))
    ),
    [thesisDiagnostics?.conflicts]
  );

  const resolveThesisScopeLabel = (conflict: RebalanceThesisConflict): string => {
    if (String(conflict.scope || '').toLowerCase() === 'assetclass') {
      const classKey = String(conflict.scope_key || '').toLowerCase();
      return t(`assets.classes.${classKey}`, { defaultValue: toTitleCase(classKey || '-') });
    }
    return String(conflict.scope_key || '-');
  };

  const resolveRowLabel = (row: RebalanceDriftItem | RebalanceSuggestionItem): string => {
    const rowScope = normalizeScope((row as RebalanceDriftItem).scope);
    if (rowScope === 'asset') {
      const rowAssetId = String((row as RebalanceDriftItem).assetId || (row as RebalanceSuggestionItem).assetId || '');
      const rowTicker = String((row as RebalanceDriftItem).ticker || (row as RebalanceSuggestionItem).ticker || '');
      if (rowTicker) return rowTicker;
      const asset = assetById[rowAssetId];
      if (asset?.ticker) return asset.ticker;
      return rowAssetId || '-';
    }

    const classKey = String((row as RebalanceDriftItem).assetClass || (row as RebalanceDriftItem).scope_key || (row as RebalanceSuggestionItem).assetClass || '');
    return t(`assets.classes.${classKey}`, { defaultValue: toTitleCase(classKey || '-') });
  };

  const targetSumState = Math.abs(percentSum - 100) < 0.0001
    ? 'balanced'
    : percentSum < 100
      ? 'under'
      : 'over';

  return (
    <Layout>
      <div className="rebalance-page">
        <div className="rebalance-page__header">
          <h1 className="rebalance-page__title">{t('rebalance.title')}</h1>
          <div className="rebalance-page__filters">
            {portfolioOptions.length > 0 && (
              <SharedDropdown
                value={selectedPortfolio}
                options={portfolioOptions}
                onChange={setSelectedPortfolio}
                ariaLabel={t('rebalance.selectPortfolio')}
                className="rebalance-page__dropdown rebalance-page__dropdown--portfolio"
                size="sm"
              />
            )}
            <SharedDropdown
              value={scope}
              options={scopeOptions}
              onChange={(value) => setScope(normalizeScope(value))}
              ariaLabel={t('rebalance.selectScope')}
              className="rebalance-page__dropdown"
              size="sm"
            />
          </div>
        </div>

        {(loadingTargets || loadingSuggestion) && (
          <div className="rebalance-page__state">{t('common.loading')}</div>
        )}

        {!loadingTargets && portfolios.length === 0 && (
          <div className="rebalance-page__state">{t('rebalance.noData')}</div>
        )}

        {!loadingTargets && error && (
          <div className="rebalance-page__state rebalance-page__state--error">
            <p>{t('rebalance.loadError')}</p>
            <code>{error}</code>
          </div>
        )}

        {!loadingTargets && !error && (
          <>
            <div className="rebalance-page__kpis">
              <article className="rebalance-kpi">
                <span className="rebalance-kpi__label">{t('rebalance.totals.currentTotal')}</span>
                <span className="rebalance-kpi__value">
                  {formatBrl(toNumber(suggestion?.current_total))}
                </span>
              </article>
              <article className="rebalance-kpi">
                <span className="rebalance-kpi__label">{t('rebalance.totals.targetTotal')}</span>
                <span className="rebalance-kpi__value">
                  {formatBrl(toNumber(suggestion?.target_total_after_contribution))}
                </span>
              </article>
              <article className="rebalance-kpi">
                <span className="rebalance-kpi__label">{t('rebalance.totals.contribution')}</span>
                <span className="rebalance-kpi__value">
                  {formatBrl(toNumber(suggestion?.contribution))}
                </span>
              </article>
            </div>

            <div className="rebalance-page__grid">
              <section className="rebalance-card rebalance-card--wide">
                <header className="rebalance-card__header">
                  <div className="rebalance-card__header-copy">
                    <h2>{t('rebalance.targetEditorTitle')}</h2>
                    <p>
                      {t('rebalance.targetSumLabel')}{' '}
                      <strong>{percentSum.toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%</strong>
                    </p>
                  </div>
                  <div className="rebalance-card__actions">
                    <button
                      className="rebalance-page__button rebalance-page__button--secondary"
                      type="button"
                      onClick={handleAddTarget}
                      disabled={scope === 'asset' && assetOptions.length === 0}
                    >
                      {t('rebalance.addTarget')}
                    </button>
                    <button
                      className="rebalance-page__button"
                      type="button"
                      onClick={handleSaveTargets}
                      disabled={savingTargets || hasAnyInvalidRows}
                    >
                      {t('rebalance.saveTargets')}
                    </button>
                  </div>
                </header>

                {scope === 'asset' && assetOptions.length === 0 ? (
                  <p className="rebalance-card__empty">{t('rebalance.messages.noAssets')}</p>
                ) : (
                  <div className="rebalance-table-wrapper">
                    <table className="rebalance-table">
                      <thead>
                        <tr>
                          <th>{scope === 'assetClass' ? t('rebalance.table.class') : t('rebalance.table.asset')}</th>
                          <th>{t('rebalance.table.percent')}</th>
                          <th>{t('rebalance.table.actions')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {currentRows.map((row) => {
                          const duplicate = valueDuplicates.has(String(row.value || '').trim().toLowerCase());
                          const percentInvalid = toNumber(row.percent) <= 0;
                          return (
                            <tr key={row.localId}>
                              <td className={duplicate ? 'rebalance-table__cell rebalance-table__cell--error' : 'rebalance-table__cell'}>
                                <SharedDropdown
                                  value={row.value}
                                  options={currentOptions}
                                  onChange={(value) => handleTargetChange(row.localId, { value })}
                                  ariaLabel={scope === 'assetClass' ? t('rebalance.table.class') : t('rebalance.table.asset')}
                                  size="sm"
                                  disabled={currentOptions.length === 0}
                                />
                              </td>
                              <td className={percentInvalid ? 'rebalance-table__cell rebalance-table__cell--error' : 'rebalance-table__cell'}>
                                <input
                                  className="rebalance-page__input"
                                  value={row.percent}
                                  onChange={(event) => handleTargetChange(row.localId, { percent: event.target.value })}
                                  inputMode="decimal"
                                  type="number"
                                  min="0"
                                  step="0.01"
                                />
                              </td>
                              <td>
                                <button
                                  type="button"
                                  className="rebalance-page__button rebalance-page__button--danger"
                                  onClick={() => handleRemoveTarget(row.localId)}
                                >
                                  {t('common.delete')}
                                </button>
                              </td>
                            </tr>
                          );
                        })}
                        {currentRows.length === 0 && (
                          <tr>
                            <td colSpan={3} className="rebalance-table__empty">{t('rebalance.emptyTargets')}</td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                )}

                <footer className="rebalance-card__footer">
                  <span className={`rebalance-badge rebalance-badge--${targetSumState}`}>
                    {t(`rebalance.targetSumState.${targetSumState}`)}
                  </span>
                  {currentScopeHasInvalidRows && (
                    <span className="rebalance-badge rebalance-badge--danger">{t('rebalance.messages.invalidTargets')}</span>
                  )}
                  {notice && <span className="rebalance-badge rebalance-badge--success">{notice}</span>}
                </footer>
              </section>

              <section className="rebalance-card">
                <header className="rebalance-card__header">
                  <h2>{t('rebalance.suggestionTitle')}</h2>
                </header>
                <div className="rebalance-page__contribution">
                  <label htmlFor="rebalance-contribution-brl">{t('rebalance.amountBrl')}</label>
                  <input
                    id="rebalance-contribution-brl"
                    className="rebalance-page__input"
                    value={contributionAmountBrl}
                    onChange={(event) => setContributionAmountBrl(event.target.value)}
                    inputMode="decimal"
                    type="number"
                    min="0"
                    step="0.01"
                  />
                  <label htmlFor="rebalance-contribution-usd">{t('rebalance.amountUsd')}</label>
                  <input
                    id="rebalance-contribution-usd"
                    className="rebalance-page__input"
                    value={contributionAmountUsd}
                    onChange={(event) => setContributionAmountUsd(event.target.value)}
                    inputMode="decimal"
                    type="number"
                    min="0"
                    step="0.01"
                  />
                  <p className="rebalance-page__contribution-hint">{t('rebalance.contributionRoutingHint')}</p>
                  {suggestion?.contribution_input ? (
                    <p className="rebalance-page__contribution-summary">
                      {t('rebalance.convertedContribution', {
                        total: formatBrl(toNumber(suggestion.contribution_input.total_brl)),
                        fx: toNumber(suggestion.contribution_input.usd_brl_rate).toLocaleString(numberLocale, {
                          minimumFractionDigits: 2,
                          maximumFractionDigits: 4,
                        }),
                      })}
                    </p>
                  ) : null}
                  <button
                    type="button"
                    className="rebalance-page__button"
                    onClick={runSuggestion}
                    disabled={loadingSuggestion}
                  >
                    {t('rebalance.refreshSuggestion')}
                  </button>
                </div>
              </section>

              <section className="rebalance-card rebalance-card--wide">
                <header className="rebalance-card__header">
                  <h2>{t('rebalance.thesis.title')}</h2>
                </header>
                {!thesisDiagnostics ? (
                  <p className="rebalance-card__empty">{t('rebalance.thesis.unavailable')}</p>
                ) : (
                  <div className="rebalance-thesis">
                    <div className="rebalance-thesis__stats">
                      <article className="rebalance-thesis__stat">
                        <span>{t('rebalance.thesis.source')}</span>
                        <strong>{t(`rebalance.thesis.sources.${targetSource}`, { defaultValue: targetSource })}</strong>
                      </article>
                      <article className="rebalance-thesis__stat">
                        <span>{t('rebalance.thesis.activeScopes')}</span>
                        <strong>{toNumber(thesisDiagnostics.active_scope_count)}</strong>
                      </article>
                      <article className="rebalance-thesis__stat">
                        <span>{t('rebalance.thesis.coveredAssets')}</span>
                        <strong>
                          {`${toNumber(thesisDiagnostics.covered_asset_count)}/${toNumber(thesisDiagnostics.tracked_asset_count)}`}
                        </strong>
                      </article>
                      <article className="rebalance-thesis__stat">
                        <span>{t('rebalance.thesis.coveredValue')}</span>
                        <strong>{formatPercent(toNumber(thesisDiagnostics.covered_value_pct), 1)}</strong>
                      </article>
                    </div>
                    {(thesisDiagnostics.uncovered_scope_keys || []).length > 0 && (
                      <p className="rebalance-thesis__hint">
                        {t('rebalance.thesis.uncoveredHint', {
                          count: (thesisDiagnostics.uncovered_scope_keys || []).length,
                        })}
                      </p>
                    )}
                    {thesisConflictRows.length > 0 ? (
                      <div className="rebalance-table-wrapper">
                        <table className="rebalance-table">
                          <thead>
                            <tr>
                              <th>{t('rebalance.thesis.table.scope')}</th>
                              <th>{t('rebalance.thesis.table.issue')}</th>
                              <th>{t('rebalance.thesis.table.actual')}</th>
                              <th>{t('rebalance.thesis.table.min')}</th>
                              <th>{t('rebalance.thesis.table.max')}</th>
                              <th>{t('rebalance.thesis.table.target')}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {thesisConflictRows.map((conflict, index) => (
                              <tr key={`${conflict.scope_key}-${conflict.type}-${index}`}>
                                <td>{resolveThesisScopeLabel(conflict)}</td>
                                <td>{t(`rebalance.thesis.conflictTypes.${conflict.type}`, { defaultValue: conflict.type })}</td>
                                <td>{formatPercent(conflict.actual_pct)}</td>
                                <td>{formatPercent(conflict.min_pct)}</td>
                                <td>{formatPercent(conflict.max_pct)}</td>
                                <td>{formatPercent(conflict.target_pct)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <p className="rebalance-card__empty">{t('rebalance.thesis.noConflicts')}</p>
                    )}
                  </div>
                )}
              </section>

              <section className="rebalance-card rebalance-card--wide">
                <header className="rebalance-card__header">
                  <h2>{t('rebalance.driftTitle')}</h2>
                </header>
                {driftRows.length === 0 ? (
                  <p className="rebalance-card__empty">{t('rebalance.messages.emptyDrift')}</p>
                ) : (
                  <div className="rebalance-table-wrapper">
                    <table className="rebalance-table">
                      <thead>
                        <tr>
                          <th>{scope === 'assetClass' ? t('rebalance.table.class') : t('rebalance.table.asset')}</th>
                          <th>{t('rebalance.table.currentValue')}</th>
                          <th>{t('rebalance.table.targetValue')}</th>
                          <th>{t('rebalance.table.diffValue')}</th>
                          <th>{t('rebalance.table.driftPct')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {driftRows.map((row) => (
                          <tr key={`${row.scope}-${row.scope_key}`}>
                            <td>{resolveRowLabel(row)}</td>
                            <td>{formatBrl(toNumber(row.current_value))}</td>
                            <td>{formatBrl(toNumber(row.target_value))}</td>
                            <td className={toNumber(row.drift_value) < 0 ? 'rebalance-table__value rebalance-table__value--negative' : toNumber(row.drift_value) > 0 ? 'rebalance-table__value rebalance-table__value--positive' : 'rebalance-table__value'}>
                              {formatSignedBrl(toNumber(row.drift_value))}
                            </td>
                            <td className={toNumber(row.drift_pct) < 0 ? 'rebalance-table__value rebalance-table__value--negative' : toNumber(row.drift_pct) > 0 ? 'rebalance-table__value rebalance-table__value--positive' : 'rebalance-table__value'}>
                              {`${toNumber(row.drift_pct).toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </section>

              <section className="rebalance-card rebalance-card--wide">
                <header className="rebalance-card__header">
                  <h2>{t('rebalance.table.recommendedContribution')}</h2>
                </header>
                {suggestionRows.length === 0 ? (
                  <p className="rebalance-card__empty">{t('rebalance.messages.emptySuggestion')}</p>
                ) : (
                  <div className="rebalance-table-wrapper">
                    <table className="rebalance-table">
                      <thead>
                        <tr>
                          <th>{scope === 'assetClass' ? t('rebalance.table.class') : t('rebalance.table.asset')}</th>
                          <th>{t('rebalance.table.currentValue')}</th>
                          <th>{t('rebalance.table.targetValue')}</th>
                          <th>{t('rebalance.table.recommendedContribution')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {suggestionRows.map((row, index) => (
                          <tr key={`${String(row.assetId || row.assetClass || index)}-${index}`}>
                            <td>{resolveRowLabel(row)}</td>
                            <td>{formatBrl(toNumber(row.current_value))}</td>
                            <td>{formatBrl(toNumber(row.target_value))}</td>
                            <td className="rebalance-table__value rebalance-table__value--positive">
                              {formatBrl(toNumber(row.recommended_amount))}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </section>
            </div>
          </>
        )}
      </div>
    </Layout>
  );
};

export default RebalancePage;
