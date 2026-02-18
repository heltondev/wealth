import { useTranslation } from 'react-i18next';
import { useCallback, useEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import Layout from '../components/Layout';
import DataTable from '../components/DataTable';
import ExpandableText from '../components/ExpandableText';
import SharedDropdown from '../components/SharedDropdown';
import { api, type Asset, type Transaction } from '../services/api';
import { usePortfolioData } from '../context/PortfolioDataContext';
import { formatCurrency, formatDate } from '../utils/formatters';
import './AssetsPage.scss';
import './AssetDetailsPage.scss';

type AssetRow = Asset & {
  quantity: number;
  source: string | null;
  investedAmount: number;
};

type AssetTradeHistoryRow = {
  transId: string;
  date: string;
  type: 'buy' | 'sell';
  quantity: number;
  price: number;
  amount: number;
  currency: string;
  source: string | null;
};

type AssetTradeHistoryPoint = AssetTradeHistoryRow & {
  x: number;
  y: number;
  index: number;
};

type AssetPriceSeriesPoint = {
  date: string;
  display_date: string | undefined;
  close: number | null;
  stock_splits: number | null;
};

type AssetSplitEvent = {
  date: string;
  displayDate: string;
  factor: number;
  eventType: 'desdobramento' | 'grupamento';
};

type ChartPeriodPreset = 'MAX' | '5A' | '2A' | '1A' | '6M' | '3M' | '1M' | 'CUSTOM';
type FinancialStatementKind = 'income' | 'balance' | 'cashflow';
type FinancialFrequency = 'annual' | 'quarterly';
type AssetDetailsTab = 'overview' | 'portfolio' | 'emissions' | 'financials' | 'history' | 'news';
type AssetInsightsSnapshot = {
  status: 'loading' | 'ready' | 'error';
  source: string | null;
  fetchedAt: string | null;
  sector: string | null;
  industry: string | null;
  marketCap: number | null;
  averageVolume: number | null;
  currentPrice: number | null;
  graham: number | null;
  bazin: number | null;
  fairPrice: number | null;
  marginOfSafetyPct: number | null;
  pe: number | null;
  pb: number | null;
  roe: number | null;
  roa: number | null;
  roic: number | null;
  payout: number | null;
  evEbitda: number | null;
  netDebtEbitda: number | null;
  lpa: number | null;
  vpa: number | null;
  netMargin: number | null;
  ebitMargin: number | null;
  statusInvestUrl: string | null;
  b3Url: string | null;
  clubeFiiUrl: string | null;
  fiisUrl: string | null;
  errorMessage: string | null;
};

type AssetFinancialStatements = {
  financials?: unknown;
  quarterly_financials?: unknown;
  balance_sheet?: unknown;
  quarterly_balance_sheet?: unknown;
  cashflow?: unknown;
  quarterly_cashflow?: unknown;
  documents?: unknown;
  fund_info?: unknown;
  fund_portfolio?: unknown;
};

type AssetFinancialDocument = {
  id: string | null;
  source: string | null;
  title: string | null;
  category: string | null;
  documentType: string | null;
  referenceDate: string | null;
  deliveryDate: string | null;
  status: string | null;
  url: string;
  viewerUrl: string | null;
  downloadUrl: string | null;
};

type AssetFundGeneralInfo = {
  legalName: string | null;
  tradingName: string | null;
  acronym: string | null;
  cnpj: string | null;
  description: string | null;
  descriptionHtml: string | null;
  dividendsResume: AssetFundDividendsResume | null;
  dividendYieldComparator: AssetFundDividendYieldComparator | null;
  classification: string | null;
  segment: string | null;
  administrator: string | null;
  managerName: string | null;
  bookkeeper: string | null;
  website: string | null;
  address: string | null;
  phone: string | null;
  quotaCount: number | null;
  quotaDateApproved: string | null;
  tradingCode: string | null;
  tradingCodeOthers: string | null;
  b3DetailsUrl: string | null;
  source: string | null;
};

type AssetFundDividendsResumeTable = {
  periods: string[];
  returnByUnitLabel: string | null;
  returnByUnit: string[];
  relativeToQuoteLabel: string | null;
  relativeToQuote: string[];
};

type AssetFundDividendsResume = {
  title: string | null;
  paragraphs: string[];
  table: AssetFundDividendsResumeTable | null;
  source: string | null;
};

type AssetFundDividendYieldComparatorItem = {
  kind: 'principal' | 'sector' | 'category' | 'market' | 'other';
  label: string | null;
  detail: string | null;
  value: string | null;
  score: number | null;
};

type AssetFundDividendYieldComparator = {
  title: string | null;
  description: string | null;
  items: AssetFundDividendYieldComparatorItem[];
  source: string | null;
};

type AssetFundPortfolioRow = {
  label: string;
  allocationPct: number;
  category: string | null;
  source: string | null;
};

type FiiEmissionRow = {
  ticker: string;
  emissionNumber: number;
  stage: string;
  price: number | null;
  discount: number | null;
  baseDate: string;
  proportionFactor: string;
  preferenceStart: string;
  preferenceEnd: string;
  preferenceStatus: string;
  sobrasStart: string;
  sobrasEnd: string;
  sobrasStatus: string;
  publicStart: string;
  publicEnd: string;
  publicStatus: string;
};

type AssetNewsItem = {
  id: string;
  ticker: string | null;
  title: string;
  description: string | null;
  imageUrl: string | null;
  link: string;
  publishedAt: string | null;
  dataSource: string | null;
};

type AssetNewsScoredItem = AssetNewsItem & {
  relevanceScore: number;
  publishedAtTs: number;
  hasTickerMatch: boolean;
  sourceLabel: string;
};

type ParsedFinancialRow = {
  key: string;
  label: string;
  valuesByPeriod: Record<string, number | null>;
};

type ParsedFinancialStatement = {
  periods: string[];
  rows: ParsedFinancialRow[];
};

type FinancialSeriesPoint = {
  period: string;
  label: string;
  value: number | null;
};

type PortfolioCityAllocation = {
  cityKey: string;
  city: string;
  allocationPct: number;
  sources: string[];
};

type PortfolioCityCoordinate = {
  lat: number;
  lon: number;
  displayName: string | null;
};

type PortfolioCityPoint = PortfolioCityAllocation & PortfolioCityCoordinate;

type LeafletMap = {
  remove: () => void;
  removeLayer: (layer: LeafletLayerGroup) => void;
  fitBounds: (bounds: unknown, options?: { padding?: [number, number]; maxZoom?: number }) => void;
  setView: (center: [number, number], zoom: number) => void;
  invalidateSize: () => void;
};

type LeafletMarker = {
  bindPopup: (content: string) => LeafletMarker;
};

type LeafletLayerGroup = {
  addTo: (map: LeafletMap) => LeafletLayerGroup;
};

type LeafletFeatureGroup = {
  getBounds: () => unknown;
};

type LeafletScaleControl = {
  addTo: (map: LeafletMap) => LeafletScaleControl;
};

type LeafletRuntime = {
  map: (
    container: HTMLElement,
    options?: { zoomControl?: boolean; scrollWheelZoom?: boolean }
  ) => LeafletMap;
  tileLayer: (
    urlTemplate: string,
    options?: { attribution?: string }
  ) => { addTo: (map: LeafletMap) => unknown };
  marker: (latlng: [number, number]) => LeafletMarker;
  layerGroup: (layers: LeafletMarker[]) => LeafletLayerGroup;
  featureGroup: (layers: LeafletMarker[]) => LeafletFeatureGroup;
  control: {
    scale: (options?: { imperial?: boolean }) => LeafletScaleControl;
  };
};

declare global {
  interface Window {
    L?: LeafletRuntime;
  }
}

const LEAFLET_CSS_ID = 'asset-details-leaflet-css';
const LEAFLET_SCRIPT_ID = 'asset-details-leaflet-script';
const LEAFLET_SCRIPT_URL = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js';
const LEAFLET_CSS_URL = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
const BRAZIL_MAP_DEFAULT_CENTER: [number, number] = [-14.235, -51.9253];
const BRAZIL_MAP_DEFAULT_ZOOM = 4;
const CITY_GEOCODE_LIMIT = 12;

let leafletLoaderPromise: Promise<LeafletRuntime | null> | null = null;

const ensureLeafletLoaded = () => {
  if (typeof window === 'undefined' || typeof document === 'undefined') {
    return Promise.resolve(null);
  }
  if (window.L) return Promise.resolve(window.L);
  if (leafletLoaderPromise) return leafletLoaderPromise;

  if (!document.getElementById(LEAFLET_CSS_ID)) {
    const link = document.createElement('link');
    link.id = LEAFLET_CSS_ID;
    link.rel = 'stylesheet';
    link.href = LEAFLET_CSS_URL;
    document.head.appendChild(link);
  }

  leafletLoaderPromise = new Promise((resolve, reject) => {
    const resolveLeaflet = () => {
      if (window.L) {
        resolve(window.L);
        return;
      }
      reject(new Error('leaflet_load_failed'));
    };

    const existingScript = document.getElementById(LEAFLET_SCRIPT_ID) as HTMLScriptElement | null;
    if (existingScript) {
      if (window.L) {
        resolve(window.L);
        return;
      }
      existingScript.addEventListener('load', resolveLeaflet, { once: true });
      existingScript.addEventListener('error', () => reject(new Error('leaflet_load_failed')), { once: true });
      return;
    }

    const script = document.createElement('script');
    script.id = LEAFLET_SCRIPT_ID;
    script.src = LEAFLET_SCRIPT_URL;
    script.async = true;
    script.onload = resolveLeaflet;
    script.onerror = () => reject(new Error('leaflet_load_failed'));
    document.body.appendChild(script);
  });

  return leafletLoaderPromise;
};

const COUNTRY_FLAG_MAP: Record<string, string> = {
  BR: '🇧🇷',
  US: '🇺🇸',
  CA: '🇨🇦',
};

const COUNTRY_NAME_MAP: Record<string, string> = {
  BR: 'Brazil',
  US: 'United States',
  CA: 'Canada',
};

const DECIMAL_PRECISION = 2;
const DECIMAL_FACTOR = 10 ** DECIMAL_PRECISION;
const HISTORY_CHART_WIDTH = 860;
const HISTORY_CHART_HEIGHT = 220;
const CHART_PERIOD_DAYS: Partial<Record<ChartPeriodPreset, number>> = {
  '1M': 30,
  '3M': 90,
  '6M': 180,
  '1A': 365,
  '2A': 730,
  '5A': 1825,
};
const SPLIT_EVENT_DEDUP_WINDOW_DAYS = 7;
const NEWS_MAX_ITEMS = 24;
const NEWS_RELEVANCE_MIN_SCORE = 6;
const NEWS_RELEVANCE_MIN_SCORE_RELAXED = 4;
const NEWS_TILES_MIN = 1;
const NEWS_TILES_MAX = 4;
const NEWS_TILES_DEFAULT = 3;
const NEWS_TILES_STORAGE_KEY = 'asset-details:news-tiles-per-row';
const NEWS_NAME_STOPWORDS = new Set([
  'a',
  'acao',
  'acoes',
  'asset',
  'assets',
  'corp',
  'corporation',
  'de',
  'do',
  'dos',
  'da',
  'das',
  'e',
  'em',
  'etf',
  'fii',
  'fundo',
  'fundos',
  'fund',
  'funds',
  'imobiliario',
  'imobiliarios',
  'inc',
  'investment',
  'investments',
  'investimento',
  'investimentos',
  'ltda',
  'na',
  'no',
  'ordinarias',
  'on',
  'ou',
  'para',
  'plc',
  'pn',
  'preferenciais',
  'reit',
  'sa',
  'stock',
  'stocks',
  'trust',
  'unit',
  'units',
]);

const FINANCIAL_STATEMENT_KEY_MAP: Record<
FinancialStatementKind,
Record<FinancialFrequency, keyof AssetFinancialStatements>
> = {
  income: {
    annual: 'financials',
    quarterly: 'quarterly_financials',
  },
  balance: {
    annual: 'balance_sheet',
    quarterly: 'quarterly_balance_sheet',
  },
  cashflow: {
    annual: 'cashflow',
    quarterly: 'quarterly_cashflow',
  },
};

const toIsoDate = (value: string) => {
  const parsed = new Date(`${value}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

const toNumericValue = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string' && value.trim() === '') return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toObjectRecord = (value: unknown): Record<string, unknown> => (
  value && typeof value === 'object' ? value as Record<string, unknown> : {}
);

const toNonEmptyString = (value: unknown): string | null => {
  const text = String(value || '').trim();
  return text || null;
};

const firstFiniteNumber = (...values: unknown[]): number | null => {
  for (const candidate of values) {
    const numeric = toNumericValue(candidate);
    if (numeric !== null) return numeric;
  }
  return null;
};

const normalizeRatioMetric = (value: number | null): number | null => {
  if (value === null || !Number.isFinite(value)) return null;
  return Math.abs(value) > 1.5 ? value / 100 : value;
};

const normalizePercentValueToRatio = (value: number | null): number | null => {
  if (value === null || !Number.isFinite(value)) return null;
  return Math.abs(value) > 1 ? value / 100 : value;
};

const toTickerSlug = (ticker: string): string => (
  String(ticker || '')
    .toLowerCase()
    .replace(/\.sa$/i, '')
    .replace(/[^a-z0-9]/g, '')
);

const toDisplayLabel = (value: string): string => (
  String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .split(' ')
    .map((part) => `${part.slice(0, 1).toUpperCase()}${part.slice(1).toLowerCase()}`)
    .join(' ')
);

const SUMMARY_CONTINUATION_TAIL = /\b(?:do|da|de|em|no|na|nos|nas|ao|aos|para|com|e|ou|o|a|os|as|um|uma|seu|sua|seus|suas|pelo|pela|pelos|pelas|dos|das|que|por|sobre)$/i;
const SUMMARY_HEADING_BREAK_TOKENS = [
  'Características do fundo',
  'Agora falando da política',
  'As aplicações realizadas',
  'Portanto,',
  'Para conferir outros dados',
];

const decodeBasicHtmlEntities = (value: string): string => (
  String(value || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&apos;/gi, '\'')
    .replace(/&#39;/gi, '\'')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
);

const stripSummaryMarkup = (value: string): string => (
  decodeBasicHtmlEntities(String(value || ''))
    .replace(/<[^>]+>/g, ' ')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
);

const reflowSummaryBlocks = (value: unknown): string[] => {
  const raw = String(value || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .trim();
  if (!raw) return [];

  const normalizedBlocks = raw
    .split(/\n\s*\n+/)
    .map((block) => block.replace(/\n+/g, ' ').replace(/\s+/g, ' ').trim())
    .filter(Boolean);

  const shouldMergeBlock = (previousBlock: string, nextBlock: string): boolean => {
    const previousText = stripSummaryMarkup(previousBlock);
    const nextText = stripSummaryMarkup(nextBlock);
    if (!previousText || !nextText) return false;

    if (/[.!?;:)]$/.test(previousText)) return false;
    if (/<\/h[1-6]>\s*$/i.test(previousBlock)) return false;
    if (/^<\s*(h[1-6]|ul|ol|li)\b/i.test(nextBlock)) return false;

    const nextStartsLowercase = /^[a-zà-ÿ]/.test(nextText);
    const previousEndsConnector = SUMMARY_CONTINUATION_TAIL.test(previousText);
    return nextStartsLowercase || previousEndsConnector;
  };

  const mergedBlocks: string[] = [];
  for (const block of normalizedBlocks) {
    const previousIndex = mergedBlocks.length - 1;
    if (previousIndex >= 0 && shouldMergeBlock(mergedBlocks[previousIndex], block)) {
      mergedBlocks[previousIndex] = `${mergedBlocks[previousIndex]} ${block}`.trim();
      continue;
    }
    mergedBlocks.push(block);
  }

  return mergedBlocks;
};

const splitLongSummaryParagraph = (value: string): string[] => {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return [];

  const headingBreakRegex = new RegExp(
    `\\s+(?=${SUMMARY_HEADING_BREAK_TOKENS.map((token) => escapeRegExp(token)).join('|')})`,
    'gi'
  );
  const withHeadingBreaks = text.replace(headingBreakRegex, '\n\n');
  const seededChunks = withHeadingBreaks
    .split(/\n\s*\n+/)
    .map((chunk) => chunk.trim())
    .filter(Boolean);

  const result: string[] = [];
  for (const chunk of seededChunks) {
    if (chunk.length <= 420) {
      result.push(chunk);
      continue;
    }

    const sentences = chunk
      .split(/(?<=[.!?])\s+(?=[A-ZÀ-Ý])/)
      .map((entry) => entry.trim())
      .filter(Boolean);
    if (sentences.length <= 1) {
      result.push(chunk);
      continue;
    }

    let current = '';
    for (const sentence of sentences) {
      const candidate = current ? `${current} ${sentence}` : sentence;
      if (candidate.length > 360 && current) {
        result.push(current);
        current = sentence;
      } else {
        current = candidate;
      }
    }
    if (current) result.push(current);
  }

  return result;
};

const sanitizeSummaryHtml = (value: unknown): string | null => {
  const raw = String(value || '').trim();
  if (!raw) return null;

  const allowedTags = new Set([
    'article',
    'h3',
    'b',
    'strong',
    'em',
    'i',
    'p',
    'br',
    'ul',
    'ol',
    'li',
    'a',
  ]);

  let html = raw
    .replace(/<!--[\s\S]*?-->/g, '')
    .replace(/<\s*(script|style|iframe|object|embed|form|svg|math|template)[^>]*>[\s\S]*?<\s*\/\s*\1\s*>/gi, '')
    .replace(/\s(on[a-z]+)\s*=\s*(['"]).*?\2/gi, '')
    .replace(/\sstyle\s*=\s*(['"]).*?\1/gi, '')
    .replace(/\s(srcdoc)\s*=\s*(['"]).*?\2/gi, '')
    .replace(/\s(href|src)\s*=\s*(['"])\s*javascript:[\s\S]*?\2/gi, '');

  html = html.replace(/<\/?([a-zA-Z0-9]+)(\s[^>]*)?>/g, (full, tagName) => {
    const normalizedTag = String(tagName || '').toLowerCase();
    if (!allowedTags.has(normalizedTag)) return '';
    return full.trim().startsWith('</') ? `</${normalizedTag}>` : `<${normalizedTag}>`;
  });

  const articleBody = html
    .replace(/<\/?article>/gi, '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/\r/g, '');

  const blocks = reflowSummaryBlocks(articleBody);
  const formattedBlocks = blocks.map((block) => {
    if (/^<(h[1-6]|p|ul|ol)\b/i.test(block)) return block;
    return `<p>${block}</p>`;
  });

  const formattedHtml = `<article>${formattedBlocks.join('')}</article>`;
  const text = stripSummaryMarkup(formattedHtml);
  if (!text) return null;
  return formattedHtml;
};

const normalizeSummaryTextParagraphs = (value: unknown): string[] => (
  reflowSummaryBlocks(value)
    .map((block) => stripSummaryMarkup(block))
    .flatMap((block) => splitLongSummaryParagraph(block))
    .map((block) => block.replace(/\s+/g, ' ').trim())
    .filter(Boolean)
);

const escapeRegExp = (value: string): string => (
  String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
);

const stripFundSummaryIntroLines = (
  paragraphs: string[],
  ticker: string | null,
  legalName: string | null
): string[] => {
  const cleanedTicker = String(ticker || '').trim().toUpperCase();
  const cleanedLegalName = String(legalName || '').trim().toUpperCase();
  if (!cleanedTicker) return paragraphs;

  const headingPattern = new RegExp(`^${escapeRegExp(cleanedTicker)}\\s*:`, 'i');
  const taglinePattern = new RegExp(`^${escapeRegExp(cleanedTicker)}\\s*[–-]\\s*um\\b`, 'i');
  const legalNamePattern = /\bFUNDO\s+DE\s+INVESTIMENTO\b/i;
  const mainStartPatterns = [
    new RegExp(`\\b${escapeRegExp(cleanedTicker)}\\s+é\\b`, 'i'),
    new RegExp(`\\bo\\s+objeto\\s+do\\s+${escapeRegExp(cleanedTicker)}\\b`, 'i'),
  ];

  const next = [...paragraphs];
  while (next.length > 0) {
    const current = String(next[0] || '').replace(/\s+/g, ' ').trim();
    if (!current) {
      next.shift();
      continue;
    }

    const currentUpper = current.toUpperCase();
    const isTickerHeading = headingPattern.test(current);
    const isTagline = taglinePattern.test(current);
    const isLegalNameLine = Boolean(
      legalNamePattern.test(current) &&
      (currentUpper === cleanedLegalName || currentUpper.includes('RESPONSABILIDADE LIMITADA'))
    );

    // If intro and body were merged into one paragraph, keep body from first meaningful sentence.
    if (isTickerHeading || isTagline || isLegalNameLine) {
      const bodyStartIndex = mainStartPatterns
        .map((pattern) => current.search(pattern))
        .filter((index) => index > 0)
        .sort((left, right) => left - right)[0];
      if (Number.isFinite(bodyStartIndex)) {
        const trimmed = current.slice(bodyStartIndex).trim();
        if (trimmed.length > 0) {
          next[0] = trimmed;
          break;
        }
      }
    }

    if (isTickerHeading || isTagline || isLegalNameLine) {
      next.shift();
      continue;
    }

    break;
  }

  return next;
};

const toStringArray = (value: unknown): string[] => {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => toNonEmptyString(entry))
    .filter((entry): entry is string => Boolean(entry));
};

const parseFundDividendsResumePayload = (payload: unknown): AssetFundDividendsResume | null => {
  const entry = toObjectRecord(payload);
  if (Object.keys(entry).length === 0) return null;

  const title = toNonEmptyString(entry.title ?? entry.titulo ?? entry.heading);
  const paragraphs = toStringArray(entry.paragraphs).map((paragraph) => (
    paragraph.replace(/\s+/g, ' ').trim()
  ));

  const rawTable = toObjectRecord(entry.table);
  let table: AssetFundDividendsResumeTable | null = null;
  if (Object.keys(rawTable).length > 0) {
    const periods = toStringArray(rawTable.periods ?? rawTable.periodos);
    const returnByUnit = toStringArray(rawTable.return_by_unit ?? rawTable.returnByUnit);
    const relativeToQuote = toStringArray(rawTable.relative_to_quote ?? rawTable.relativeToQuote);
    const columns = Math.min(periods.length, returnByUnit.length, relativeToQuote.length);
    if (columns > 0) {
      table = {
        periods: periods.slice(0, columns),
        returnByUnitLabel: toNonEmptyString(rawTable.return_by_unit_label ?? rawTable.returnByUnitLabel),
        returnByUnit: returnByUnit.slice(0, columns),
        relativeToQuoteLabel: toNonEmptyString(rawTable.relative_to_quote_label ?? rawTable.relativeToQuoteLabel),
        relativeToQuote: relativeToQuote.slice(0, columns),
      };
    }
  }

  const source = toNonEmptyString(entry.source);
  if (!title && paragraphs.length === 0 && !table) return null;
  return {
    title,
    paragraphs,
    table,
    source,
  };
};

const parseDividendYieldComparatorKind = (
  value: unknown
): AssetFundDividendYieldComparatorItem['kind'] => {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'principal') return 'principal';
  if (normalized === 'sector' || normalized === 'setor' || normalized === 'fundos') return 'sector';
  if (normalized === 'category' || normalized === 'categoria' || normalized === 'papel') return 'category';
  if (normalized === 'market' || normalized === 'mercado' || normalized === 'ifix') return 'market';
  return 'other';
};

const normalizeScoreToPercent = (value: number | null): number | null => {
  if (value === null || !Number.isFinite(value)) return null;
  const normalized = Math.abs(value) <= 1 ? value * 100 : value;
  return Math.max(0, Math.min(100, normalized));
};

const parseFundDividendYieldComparatorPayload = (
  payload: unknown
): AssetFundDividendYieldComparator | null => {
  const entry = toObjectRecord(payload);
  if (Object.keys(entry).length === 0) return null;

  const title = toNonEmptyString(entry.title ?? entry.titulo ?? entry.heading);
  const description = toNonEmptyString(entry.description ?? entry.descricao ?? entry.text);
  const items = Array.isArray(entry.items) ? entry.items.map((rawItem) => {
    const item = toObjectRecord(rawItem);
    if (Object.keys(item).length === 0) return null;

    const kind = parseDividendYieldComparatorKind(item.kind ?? item.type);
    const label = toNonEmptyString(item.label ?? item.title ?? item.name);
    const detail = toNonEmptyString(item.detail ?? item.subtitle ?? item.context);
    const value = toNonEmptyString(item.value ?? item.dy ?? item.yield);
    const score = normalizeScoreToPercent(
      firstFiniteNumber(item.score, item.score_percent, item.scorePercent, item.progress)
    );

    if (!label && !value && score === null) return null;
    return {
      kind,
      label,
      detail,
      value,
      score,
    };
  }).filter((item): item is AssetFundDividendYieldComparatorItem => Boolean(item)) : [];
  const source = toNonEmptyString(entry.source);

  if (!title && !description && items.length === 0) return null;
  return {
    title,
    description,
    items,
    source,
  };
};

const normalizeComparableText = (value: unknown): string => (
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\.sa$/i, '')
    .replace(/\s+/g, ' ')
);

const isStockAssetClass = (assetClass: unknown): boolean => {
  const normalized = String(assetClass || '').toLowerCase();
  return normalized === 'stock' || normalized === 'equity';
};

const normalizeCityLabel = (value: unknown): string | null => {
  const text = String(value || '')
    .replace(/[|]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  if (!text) return null;

  // Ignore generic portfolio buckets that are not geographic locations.
  const lowered = text.toLowerCase();
  const blockedTokens = ['total investido', 'informe', 'segmento', 'setor', 'categoria', 'outros'];
  if (blockedTokens.some((token) => lowered.includes(token))) return null;

  // Normalize "Cidade - UF" / "Cidade/UF" / "Cidade, UF".
  const normalized = text
    .replace(/\s*[-/,]\s*([A-Z]{2})$/, ' $1')
    .replace(/\s+/g, ' ')
    .trim();

  if (normalized.length < 3) return null;
  return normalized;
};

const cityLabelToKey = (value: string): string => (
  value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '')
);

const buildAssetExternalLinks = (ticker: string, assetClass: string) => {
  const normalizedTicker = String(ticker || '').toUpperCase();
  const slug = toTickerSlug(normalizedTicker);
  const isFii = String(assetClass || '').toLowerCase() === 'fii';

  return {
    statusInvestUrl: slug
      ? `https://statusinvest.com.br/${isFii ? 'fundos-imobiliarios' : 'acoes'}/${slug}`
      : null,
    b3Url: isFii
      ? 'https://www.b3.com.br/pt_br/produtos-e-servicos/negociacao/renda-variavel/fundos-de-investimentos/fii/fiis-listados/'
      : null,
    clubeFiiUrl: isFii && slug ? `https://www.clubefii.com.br/fii/${slug}` : null,
    fiisUrl: isFii && slug ? `https://fiis.com.br/${slug}/` : null,
  };
};

const createEmptyInsightsSnapshot = (
  status: AssetInsightsSnapshot['status'],
  ticker: string,
  assetClass: string,
): AssetInsightsSnapshot => ({
  status,
  source: null,
  fetchedAt: null,
  sector: null,
  industry: null,
  marketCap: null,
  averageVolume: null,
  currentPrice: null,
  graham: null,
  bazin: null,
  fairPrice: null,
  marginOfSafetyPct: null,
  pe: null,
  pb: null,
  roe: null,
  roa: null,
  roic: null,
  payout: null,
  evEbitda: null,
  netDebtEbitda: null,
  lpa: null,
  vpa: null,
  netMargin: null,
  ebitMargin: null,
  ...buildAssetExternalLinks(ticker, assetClass),
  errorMessage: null,
});

const addDaysToIsoDate = (date: string, days: number): string => {
  const parsed = new Date(`${date}T00:00:00Z`);
  if (!Number.isFinite(parsed.getTime())) return date;
  parsed.setUTCDate(parsed.getUTCDate() + days);
  return parsed.toISOString().slice(0, 10);
};

const normalizeText = (value: unknown): string =>
  (value || '')
    .toString()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();

const summarizeSourceValue = (value: unknown): string | null => {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  if (normalized.includes('NUBANK') || normalized.includes('NU INVEST') || normalized.includes('NU BANK')) return 'NU BANK';
  if (normalized.includes('XP')) return 'XP';
  if (normalized.includes('ITAU')) return 'ITAU';
  if (normalized.includes('B3')) return 'B3';
  return null;
};

const normalizeNewsText = (value: unknown): string => (
  String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
);

const NEWS_TRACKING_QUERY_PARAMS = new Set([
  'utm_source',
  'utm_medium',
  'utm_campaign',
  'utm_term',
  'utm_content',
  'gclid',
  'fbclid',
  'mc_cid',
  'mc_eid',
  'igshid',
  'mkt_tok',
  'spm',
  'ref',
  'ref_src',
  'source',
  'si',
]);

const clampNewsTilesPerRow = (value: unknown): number => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return NEWS_TILES_DEFAULT;
  const rounded = Math.round(parsed);
  return Math.max(NEWS_TILES_MIN, Math.min(NEWS_TILES_MAX, rounded));
};

const canonicalizeNewsUrlForDedupe = (value: string): string => {
  const raw = String(value || '').trim();
  if (!raw) return '';

  try {
    const url = new URL(raw);
    if (url.hostname.includes('news.google.') && url.searchParams.has('url')) {
      const nestedUrl = url.searchParams.get('url');
      if (nestedUrl) return canonicalizeNewsUrlForDedupe(nestedUrl);
    }

    const params = [...url.searchParams.entries()]
      .filter(([key]) => {
        const normalizedKey = key.toLowerCase();
        return !normalizedKey.startsWith('utm_') && !NEWS_TRACKING_QUERY_PARAMS.has(normalizedKey);
      })
      .sort(([left], [right]) => left.localeCompare(right));
    const nextSearch = new URLSearchParams();
    for (const [key, val] of params) {
      nextSearch.append(key, val);
    }
    const pathname = url.pathname.replace(/\/+$/, '') || '/';
    const query = nextSearch.toString();
    return `${url.origin}${pathname}${query ? `?${query}` : ''}`;
  } catch {
    return raw.replace(/#.*$/, '');
  }
};

const normalizeNewsTitleForDedupe = (value: unknown): string => {
  const text = String(value || '').trim();
  if (!text) return '';
  const withoutSourceSuffix = text
    .replace(/\s+[|•]\s+[^|•]+$/, '')
    .replace(/\s+[-–—]\s+[^-–—]+$/, '')
    .trim();
  return normalizeNewsText(withoutSourceSuffix);
};

const toNewsPublishedDateKey = (value: string | null): string => {
  if (!value) return 'undated';
  const parsed = Date.parse(value);
  if (Number.isFinite(parsed)) {
    return new Date(parsed).toISOString().slice(0, 10);
  }
  const fallback = toIsoDate(String(value).slice(0, 10));
  return fallback || 'undated';
};

const normalizeNewsImageUrl = (value: unknown): string | null => {
  const raw = String(value || '')
    .replace(/&amp;/gi, '&')
    .trim();
  if (!raw) return null;
  if (raw.startsWith('//')) return `https:${raw}`;
  if (/^https?:\/\//i.test(raw)) return raw;
  return null;
};

const extractImageFromHtmlSnippet = (value: string | null): string | null => {
  const html = String(value || '');
  if (!html) return null;

  const imgMatch = html.match(/<img[^>]+src=["']([^"']+)["'][^>]*>/i);
  if (imgMatch?.[1]) {
    const direct = normalizeNewsImageUrl(imgMatch[1]);
    if (direct) return direct;
  }

  const imageUrlMatch = html.match(/https?:\/\/[^\s"'<>]+\.(?:avif|webp|png|jpe?g|gif)(?:\?[^\s"'<>]*)?/i);
  if (imageUrlMatch?.[0]) {
    const direct = normalizeNewsImageUrl(imageUrlMatch[0]);
    if (direct) return direct;
  }

  return null;
};

const extractImageFromUnknownValue = (value: unknown): string | null => {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return normalizeNewsImageUrl(value);

  if (Array.isArray(value)) {
    for (const entry of value) {
      const found = extractImageFromUnknownValue(entry);
      if (found) return found;
    }
    return null;
  }

  if (typeof value !== 'object') return null;
  const record = toObjectRecord(value);
  const direct = normalizeNewsImageUrl(
    record.url
    ?? record.src
    ?? record.href
    ?? record.image
    ?? record.imageUrl
    ?? record.image_url
    ?? record.thumbnail
    ?? record.thumbnailUrl
    ?? record.thumbnail_url
  );
  if (direct) return direct;

  for (const key of ['enclosure', 'thumbnails', 'images', 'media', 'media_content', 'media_thumbnail']) {
    const nested = extractImageFromUnknownValue(record[key]);
    if (nested) return nested;
  }

  return null;
};

const extractNewsImageFromEntry = (
  entry: Record<string, unknown>,
  descriptionRaw: string | null,
): string | null => {
  const priorityCandidates: unknown[] = [
    entry.image,
    entry.imageUrl,
    entry.image_url,
    entry.thumbnail,
    entry.thumbnailUrl,
    entry.thumbnail_url,
    entry.media,
    entry.media_content,
    entry['media:content'],
    entry.media_thumbnail,
    entry['media:thumbnail'],
    entry.enclosure,
    entry.content,
  ];

  for (const candidate of priorityCandidates) {
    const found = extractImageFromUnknownValue(candidate);
    if (found) return found;
  }

  return extractImageFromHtmlSnippet(descriptionRaw);
};

const tokenizeNewsText = (value: unknown): string[] => {
  const normalized = normalizeNewsText(value);
  return normalized ? normalized.split(' ') : [];
};

const buildTickerRelevanceTokens = (ticker: string | null): string[] => {
  const normalizedTicker = String(ticker || '')
    .toLowerCase()
    .replace(/\.sa$/i, '')
    .replace(/[^a-z0-9]/g, '');
  if (!normalizedTicker) return [];

  const tickerRoot = normalizedTicker.replace(/[0-9]+$/, '');
  const tokens = [normalizedTicker];
  if (tickerRoot && tickerRoot !== normalizedTicker) tokens.push(tickerRoot);
  return Array.from(new Set(tokens.filter((token) => token.length >= 2)));
};

const buildAssetNameRelevanceTokens = (assetName: string | null): string[] => (
  Array.from(
    new Set(
      tokenizeNewsText(assetName)
        .filter((token) => token.length >= 4)
        .filter((token) => !NEWS_NAME_STOPWORDS.has(token))
    )
  ).slice(0, 10)
);

const parseAssetNewsPayload = (payload: unknown): AssetNewsItem[] => {
  const payloadRecord = toObjectRecord(payload);
  const rawItems = Array.isArray(payloadRecord.items) ? payloadRecord.items : [];
  const dedupeByCanonicalLink = new Set<string>();
  const dedupeByTitleAndDate = new Set<string>();
  const dedupeByTitleSummaryAndDate = new Set<string>();

  const parsedItems = rawItems
    .map((rawItem, index) => {
      const entry = toObjectRecord(rawItem);
      const title = toNonEmptyString(entry.title);
      const link = toNonEmptyString(entry.link ?? entry.url);
      if (!title || !link) return null;

      const descriptionRaw = toNonEmptyString(entry.description ?? entry.summary ?? entry.snippet);
      const description = descriptionRaw ? stripSummaryMarkup(descriptionRaw) : null;
      const imageUrl = extractNewsImageFromEntry(entry, descriptionRaw);
      const publishedAt = toNonEmptyString(entry.publishedAt ?? entry.published_at ?? entry.pubDate);
      const dataSource = toNonEmptyString(entry.data_source ?? entry.source);
      const ticker = toNonEmptyString(entry.ticker);
      const id = toNonEmptyString(entry.id ?? entry.newsId ?? entry.SK ?? entry.sk) || `${link}|${title}|${index}`;
      const canonicalLink = canonicalizeNewsUrlForDedupe(link);
      const titleKey = normalizeNewsTitleForDedupe(title);
      const publishedDateKey = toNewsPublishedDateKey(publishedAt);
      const descriptionKey = tokenizeNewsText(description || '')
        .slice(0, 16)
        .join(' ');
      const titleAndDateKey = `${titleKey}|${publishedDateKey}`;
      const titleSummaryAndDateKey = `${titleKey}|${descriptionKey}|${publishedDateKey}`;

      if (canonicalLink && dedupeByCanonicalLink.has(canonicalLink)) return null;
      if (titleKey && dedupeByTitleAndDate.has(titleAndDateKey)) return null;
      if (titleKey && descriptionKey && dedupeByTitleSummaryAndDate.has(titleSummaryAndDateKey)) return null;

      if (canonicalLink) dedupeByCanonicalLink.add(canonicalLink);
      if (titleKey) dedupeByTitleAndDate.add(titleAndDateKey);
      if (titleKey && descriptionKey) dedupeByTitleSummaryAndDate.add(titleSummaryAndDateKey);

      return {
        id,
        ticker,
        title,
        description,
        imageUrl,
        link,
        publishedAt,
        dataSource,
      } satisfies AssetNewsItem;
    })
    .filter((entry): entry is AssetNewsItem => Boolean(entry));

  return parsedItems
    .sort((left, right) => {
      const leftTs = left.publishedAt ? Date.parse(left.publishedAt) : 0;
      const rightTs = right.publishedAt ? Date.parse(right.publishedAt) : 0;
      return rightTs - leftTs;
    })
    .slice(0, NEWS_MAX_ITEMS);
};

const scoreAssetNewsItem = (
  item: AssetNewsItem,
  tickerTokens: string[],
  assetNameTokens: string[],
): AssetNewsScoredItem => {
  const titleTokenSet = new Set(tokenizeNewsText(item.title));
  const descriptionTokenSet = new Set(tokenizeNewsText(item.description || ''));
  const linkTokens = tokenizeNewsText(item.link);
  const linkText = normalizeNewsText(linkTokens.join(' '));
  const tickerInTitle = tickerTokens.filter((token) => titleTokenSet.has(token)).length;
  const tickerInDescription = tickerTokens.filter((token) => descriptionTokenSet.has(token)).length;
  const tickerInLink = tickerTokens.filter((token) => linkText.includes(token)).length;
  const assetNameInTitle = assetNameTokens.filter((token) => titleTokenSet.has(token)).length;
  const assetNameInDescription = assetNameTokens.filter((token) => descriptionTokenSet.has(token)).length;
  const hasTickerMatch = tickerInTitle > 0 || tickerInDescription > 0 || tickerInLink > 0;

  let relevanceScore = 0;
  if (tickerInTitle > 0) relevanceScore += 8;
  if (tickerInDescription > 0) relevanceScore += 5;
  if (tickerInLink > 0) relevanceScore += 3;
  relevanceScore += Math.min(assetNameInTitle * 2, 6);
  relevanceScore += Math.min(assetNameInDescription, 3);
  if (!hasTickerMatch && tickerTokens.length > 0) relevanceScore -= 2;
  if (!hasTickerMatch && assetNameInTitle === 0 && assetNameInDescription === 0) relevanceScore -= 4;

  const parsedPublishedAt = item.publishedAt ? Date.parse(item.publishedAt) : Number.NaN;
  const publishedAtTs = Number.isFinite(parsedPublishedAt) ? parsedPublishedAt : 0;
  if (publishedAtTs > 0) {
    const ageDays = Math.max(0, (Date.now() - publishedAtTs) / (1000 * 60 * 60 * 24));
    if (ageDays <= 1) relevanceScore += 3;
    else if (ageDays <= 3) relevanceScore += 2;
    else if (ageDays <= 7) relevanceScore += 1;
    else if (ageDays > 30) relevanceScore -= 1;
  }

  return {
    ...item,
    relevanceScore,
    publishedAtTs,
    hasTickerMatch,
    sourceLabel: item.dataSource ? toDisplayLabel(item.dataSource) : '-',
  };
};

const normalizeFinancialKey = (value: string): string => (
  String(value || '')
    .replace(/[\s_-]/g, '')
    .toLowerCase()
);

const FINANCIAL_PERIOD_KEY_TOKENS = new Set([
  'date',
  'asofdate',
  'period',
  'enddate',
  'fiscaldateending',
  'reporteddate',
]);

const FINANCIAL_META_KEY_TOKENS = new Set([
  ...FINANCIAL_PERIOD_KEY_TOKENS,
  'currency',
  'currencycode',
  'reportedcurrency',
  'maxage',
]);

const FINANCIAL_ACRONYMS = new Set([
  'EBIT',
  'EBITDA',
  'EPS',
  'LPA',
  'VPA',
  'ROE',
  'ROA',
  'ROIC',
  'PPE',
  'CAPEX',
  'FFO',
  'AFFO',
]);

const normalizeFinancialPeriod = (value: unknown): string | null => {
  const text = String(value || '').trim();
  if (!text) return null;

  const quarterYearFirst = text.match(/^(\d{4})[-/ ]?Q([1-4])$/i);
  if (quarterYearFirst) {
    return `${quarterYearFirst[1]}-Q${quarterYearFirst[2]}`;
  }

  const quarterLabelFirst = text.match(/^Q([1-4])[-/ ]?(\d{4})$/i);
  if (quarterLabelFirst) {
    return `${quarterLabelFirst[2]}-Q${quarterLabelFirst[1]}`;
  }

  const isoMatch = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    return `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}`;
  }

  const yearMonthMatch = text.match(/^(\d{4})-(\d{2})$/);
  if (yearMonthMatch) {
    return `${yearMonthMatch[1]}-${yearMonthMatch[2]}-01`;
  }

  const yearMatch = text.match(/^(\d{4})$/);
  if (yearMatch) {
    return `${yearMatch[1]}-12-31`;
  }

  const parsed = new Date(text);
  if (!Number.isFinite(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

const isFinancialMetaKey = (key: string): boolean => FINANCIAL_META_KEY_TOKENS.has(normalizeFinancialKey(key));

const resolveFinancialPeriodFromRecord = (value: unknown): string | null => {
  const record = toObjectRecord(value);
  const entries = Object.entries(record);
  if (entries.length === 0) return null;

  for (const [key, fieldValue] of entries) {
    if (!FINANCIAL_PERIOD_KEY_TOKENS.has(normalizeFinancialKey(key))) continue;
    const normalized = normalizeFinancialPeriod(fieldValue);
    if (normalized) return normalized;
  }

  for (const key of Object.keys(record)) {
    const normalized = normalizeFinancialPeriod(key);
    if (normalized) return normalized;
  }

  return null;
};

const toFinancialPeriodSortKey = (period: string): number => {
  const quarterMatch = period.match(/^(\d{4})-Q([1-4])$/);
  if (quarterMatch) {
    const year = Number(quarterMatch[1]);
    const quarter = Number(quarterMatch[2]);
    return Date.UTC(year, (quarter - 1) * 3, 1);
  }

  const isoMatch = period.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    return Date.UTC(Number(isoMatch[1]), Number(isoMatch[2]) - 1, Number(isoMatch[3]));
  }

  return Number.NEGATIVE_INFINITY;
};

const toFinancialMetricLabel = (key: string): string => {
  const normalized = String(key || '')
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/\s+/g, ' ')
    .trim();
  if (!normalized) return key;

  return normalized
    .split(' ')
    .map((word) => {
      const upper = word.toUpperCase();
      if (FINANCIAL_ACRONYMS.has(upper)) return upper;
      if (upper.length <= 3 && word === upper) return upper;
      return `${word.charAt(0).toUpperCase()}${word.slice(1).toLowerCase()}`;
    })
    .join(' ');
};

const extractFinancialNumericValue = (value: unknown): number | null => {
  const direct = toNumericValue(value);
  if (direct !== null) return direct;

  const record = toObjectRecord(value);
  if (Object.keys(record).length === 0) return null;

  const reportedValue = toObjectRecord(record.reportedValue);
  const firstCandidate = firstFiniteNumber(
    record.raw,
    record.value,
    record.amount,
    record.net,
    reportedValue.raw,
    reportedValue.value,
  );
  if (firstCandidate !== null) return firstCandidate;

  for (const nested of Object.values(record)) {
    const nestedRecord = toObjectRecord(nested);
    if (Object.keys(nestedRecord).length === 0) continue;
    const nestedValue = firstFiniteNumber(
      nestedRecord.raw,
      nestedRecord.value,
      nestedRecord.amount,
    );
    if (nestedValue !== null) return nestedValue;
  }

  return null;
};

const parseFinancialStatementPayload = (payload: unknown): ParsedFinancialStatement => {
  const periodsSet = new Set<string>();
  const rowsMap = new Map<string, ParsedFinancialRow>();

  const registerValue = (metricKey: string, period: string, rawValue: unknown) => {
    if (isFinancialMetaKey(metricKey)) return;

    const normalizedMetric = String(metricKey || '').trim();
    const normalizedPeriod = normalizeFinancialPeriod(period);
    if (!normalizedMetric || !normalizedPeriod) return;

    const numeric = extractFinancialNumericValue(rawValue);
    if (numeric === null) return;

    periodsSet.add(normalizedPeriod);
    const existing = rowsMap.get(normalizedMetric);
    if (existing) {
      existing.valuesByPeriod[normalizedPeriod] = numeric;
      return;
    }

    rowsMap.set(normalizedMetric, {
      key: normalizedMetric,
      label: toFinancialMetricLabel(normalizedMetric),
      valuesByPeriod: {
        [normalizedPeriod]: numeric,
      },
    });
  };

  const parsePeriodRecord = (period: string, value: unknown) => {
    const record = toObjectRecord(value);
    for (const [metricKey, metricValue] of Object.entries(record)) {
      registerValue(metricKey, period, metricValue);
    }
  };

  const parseMetricRecord = (metricKey: string, value: unknown) => {
    const record = toObjectRecord(value);
    const entries = Object.entries(record);
    if (entries.length === 0) return;

    let foundPeriod = false;
    for (const [periodKey, periodValue] of entries) {
      const normalizedPeriod = normalizeFinancialPeriod(periodKey);
      if (!normalizedPeriod) continue;
      foundPeriod = true;
      registerValue(metricKey, normalizedPeriod, periodValue);
    }

    if (foundPeriod) return;

    const period = resolveFinancialPeriodFromRecord(record);
    if (period) {
      registerValue(metricKey, period, record.value ?? record.raw ?? record);
    }
  };

  const parseRowsArray = (rows: unknown[]) => {
    for (const row of rows) {
      const rowRecord = toObjectRecord(row);
      if (Object.keys(rowRecord).length === 0) continue;
      const period = resolveFinancialPeriodFromRecord(rowRecord);
      if (!period) continue;
      for (const [metricKey, metricValue] of Object.entries(rowRecord)) {
        registerValue(metricKey, period, metricValue);
      }
    }
  };

  if (Array.isArray(payload)) {
    parseRowsArray(payload);
  } else {
    const root = toObjectRecord(payload);
    const rootEntries = Object.entries(root);

    const periodLikeRootEntries = rootEntries.reduce((count, [key, value]) => {
      const period = normalizeFinancialPeriod(key);
      if (!period || typeof value !== 'object' || value === null) return count;
      return count + 1;
    }, 0);

    if (
      rootEntries.length > 0
      && periodLikeRootEntries > 0
      && periodLikeRootEntries >= Math.ceil(rootEntries.length / 2)
    ) {
      for (const [periodKey, periodPayload] of rootEntries) {
        const normalizedPeriod = normalizeFinancialPeriod(periodKey);
        if (!normalizedPeriod) continue;
        parsePeriodRecord(normalizedPeriod, periodPayload);
      }
    } else {
      for (const [metricKey, metricPayload] of rootEntries) {
        if (Array.isArray(metricPayload)) {
          parseRowsArray(metricPayload);
          continue;
        }
        parseMetricRecord(metricKey, metricPayload);
      }
    }
  }

  const periods = Array.from(periodsSet)
    .sort((left, right) => toFinancialPeriodSortKey(left) - toFinancialPeriodSortKey(right));

  const rows = Array.from(rowsMap.values())
    .filter((row) => periods.some((period) => row.valuesByPeriod[period] !== undefined));

  return {
    periods,
    rows,
  };
};

const normalizeFinancialDocumentUrl = (value: unknown): string | null => {
  const text = String(value || '').trim();
  if (!text) return null;
  if (/^https?:\/\//i.test(text)) return text;
  if (text.startsWith('//')) return `https:${text}`;
  if (/^[a-z0-9.-]+\.[a-z]{2,}\/?/i.test(text)) return `https://${text}`;
  return null;
};

const parseLocalizedNumber = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const direct = Number(value);
  if (Number.isFinite(direct)) return direct;

  let text = String(value).trim();
  if (!text) return null;
  text = text.replace(/[^\d,.-]/g, '');
  if (!text) return null;

  const hasComma = text.includes(',');
  const hasDot = text.includes('.');
  if (hasComma && hasDot) {
    if (text.lastIndexOf(',') > text.lastIndexOf('.')) {
      text = text.replace(/\./g, '').replace(',', '.');
    } else {
      text = text.replace(/,/g, '');
    }
  } else if (hasComma) {
    if (/,\d{1,4}$/.test(text)) {
      text = text.replace(/\./g, '').replace(',', '.');
    } else {
      text = text.replace(/,/g, '');
    }
  } else if ((text.match(/\./g) || []).length > 1) {
    text = text.replace(/\./g, '');
  }

  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
};

const normalizeSplitEventType = (value: unknown, factor: number): 'desdobramento' | 'grupamento' => {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized.includes('desdobr') || normalized.includes('split')) return 'desdobramento';
  if (normalized.includes('grup') || normalized.includes('reverse')) return 'grupamento';
  return factor > 1 ? 'desdobramento' : 'grupamento';
};

const dedupeSplitEvents = (events: AssetSplitEvent[]): AssetSplitEvent[] => {
  const dedupeWindowMs = SPLIT_EVENT_DEDUP_WINDOW_DAYS * 86400000;
  const byDate = [...events].sort((left, right) => left.date.localeCompare(right.date));
  const deduped: AssetSplitEvent[] = [];

  for (const event of byDate) {
    const eventTime = new Date(`${event.date}T00:00:00Z`).getTime();
    const duplicate = deduped.some((existing) => {
      const existingTime = new Date(`${existing.date}T00:00:00Z`).getTime();
      if (!Number.isFinite(existingTime) || !Number.isFinite(eventTime)) return false;
      const sameFactor = Math.abs(existing.factor - event.factor) <= 1e-8;
      return sameFactor && Math.abs(existingTime - eventTime) <= dedupeWindowMs;
    });
    if (duplicate) continue;
    deduped.push(event);
  }

  return deduped.sort((left, right) => right.date.localeCompare(left.date));
};

const parseFundGeneralInfoPayload = (payload: unknown): AssetFundGeneralInfo | null => {
  const entry = toObjectRecord(payload);
  if (Object.keys(entry).length === 0) return null;

  const legalName = toNonEmptyString(entry.legal_name ?? entry.legalName ?? entry.fund_name ?? entry.fundName);
  const tradingName = toNonEmptyString(entry.trading_name ?? entry.tradingName);
  const acronym = toNonEmptyString(entry.acronym);
  const cnpj = toNonEmptyString(entry.cnpj);
  const description = toNonEmptyString(entry.description ?? entry.descricao ?? entry.tudo_sobre ?? entry.tudoSobre);
  const descriptionHtml = sanitizeSummaryHtml(
    entry.description_html
    ?? entry.descriptionHtml
    ?? entry.summary_html
    ?? entry.summaryHtml
    ?? entry.descricao_html
    ?? entry.descricaoHtml
  );
  const dividendsResume = parseFundDividendsResumePayload(
    entry.dividends_resume
    ?? entry.dividendsResume
    ?? entry.dividend_resume
    ?? entry.dividendResume
  );
  const dividendYieldComparator = parseFundDividendYieldComparatorPayload(
    entry.dividend_yield_comparator
    ?? entry.dividendYieldComparator
    ?? entry.yield_comparator
    ?? entry.yieldComparator
  );
  const classification = toNonEmptyString(entry.classification);
  const segment = toNonEmptyString(entry.segment);
  const administrator = toNonEmptyString(entry.administrator);
  const managerName = toNonEmptyString(entry.manager_name ?? entry.managerName);
  const bookkeeper = toNonEmptyString(entry.bookkeeper);
  const website = normalizeFinancialDocumentUrl(entry.website);
  const address = toNonEmptyString(entry.address);
  const phone = toNonEmptyString(entry.phone);
  const quotaCount = parseLocalizedNumber(entry.quota_count ?? entry.quotaCount);
  const quotaDateApproved = toIsoDate(String(entry.quota_date_approved ?? entry.quotaDateApproved ?? '')) || null;
  const tradingCode = toNonEmptyString(entry.trading_code ?? entry.tradingCode);
  const tradingCodeOthers = toNonEmptyString(entry.trading_code_others ?? entry.tradingCodeOthers);
  const b3DetailsUrl = normalizeFinancialDocumentUrl(entry.b3_details_url ?? entry.b3DetailsUrl);
  const source = toNonEmptyString(entry.source);

  const normalized: AssetFundGeneralInfo = {
    legalName,
    tradingName,
    acronym,
    cnpj,
    description,
    descriptionHtml,
    dividendsResume,
    dividendYieldComparator,
    classification,
    segment,
    administrator,
    managerName,
    bookkeeper,
    website,
    address,
    phone,
    quotaCount,
    quotaDateApproved,
    tradingCode,
    tradingCodeOthers,
    b3DetailsUrl,
    source,
  };

  const hasValues = Object.entries(normalized).some(([key, value]) => (
    key !== 'source' && value !== null && value !== undefined && String(value).trim() !== ''
  ));
  return hasValues ? normalized : null;
};

const parseFundPortfolioPayload = (payload: unknown): AssetFundPortfolioRow[] => {
  const rawRows = Array.isArray(payload)
    ? payload
    : (toObjectRecord(payload).rows && Array.isArray(toObjectRecord(payload).rows))
      ? (toObjectRecord(payload).rows as unknown[])
      : [];
  if (rawRows.length === 0) return [];

  const dedupe = new Set<string>();
  const rows: AssetFundPortfolioRow[] = [];

  for (const rawEntry of rawRows) {
    const entry = toObjectRecord(rawEntry);
    if (Object.keys(entry).length === 0) continue;

    const label = toNonEmptyString(
      entry.label
      ?? entry.name
      ?? entry.nome
      ?? entry.segment
      ?? entry.segmento
      ?? entry.category
      ?? entry.categoria
      ?? entry.asset
      ?? entry.ativo
      ?? entry.ticker
      ?? null
    );
    if (!label) continue;

    let allocationPct = parseLocalizedNumber(
      entry.allocation_pct
      ?? entry.allocation
      ?? entry.percent
      ?? entry.percentage
      ?? entry.weight
      ?? entry.peso
      ?? entry.participacao
      ?? null
    );
    if (allocationPct === null) continue;
    if (Math.abs(allocationPct) <= 1 && allocationPct !== 0) {
      allocationPct *= 100;
    }
    if (!Number.isFinite(allocationPct) || allocationPct <= 0 || allocationPct > 100) continue;

    const categoryRaw = toNonEmptyString(
      entry.category
      ?? entry.categoria
      ?? entry.segment
      ?? entry.segmento
      ?? entry.sector
      ?? entry.setor
      ?? entry.class
      ?? entry.classe
      ?? entry.type
      ?? entry.tipo
      ?? null
    );
    const category = categoryRaw && categoryRaw !== label ? categoryRaw : null;
    const source = toNonEmptyString(entry.source);
    const normalizedAllocation = Number(allocationPct.toFixed(4));

    const dedupeKey = `${label.toLowerCase()}|${normalizedAllocation.toFixed(4)}|${(category || '').toLowerCase()}`;
    if (dedupe.has(dedupeKey)) continue;
    dedupe.add(dedupeKey);

    rows.push({
      label,
      allocationPct: normalizedAllocation,
      category,
      source,
    });
  }

  return rows.sort((left, right) => right.allocationPct - left.allocationPct);
};

const parseFinancialDocumentPayload = (payload: unknown): AssetFinancialDocument[] => {
  if (!Array.isArray(payload)) return [];

  const dedupe = new Set<string>();
  const rows: AssetFinancialDocument[] = [];

  for (const rawEntry of payload) {
    const entry = toObjectRecord(rawEntry);
    if (Object.keys(entry).length === 0) continue;

    const primaryUrl = normalizeFinancialDocumentUrl(
      entry.url
      ?? entry.url_viewer
      ?? entry.urlViewer
      ?? entry.url_download
      ?? entry.urlDownload
      ?? null
    );
    if (!primaryUrl) continue;

    const title = toNonEmptyString(entry.title) || null;
    const source = toNonEmptyString(entry.source) || null;
    const referenceDate = toIsoDate(String(entry.reference_date || entry.referenceDate || '')) || null;
    const deliveryDate = toIsoDate(String(entry.delivery_date || entry.deliveryDate || '')) || null;
    const status = toNonEmptyString(entry.status) || null;
    const category = toNonEmptyString(entry.category) || null;
    const documentType = toNonEmptyString(entry.document_type ?? entry.documentType) || null;
    const id = toNonEmptyString(entry.id) || null;
    const viewerUrl = normalizeFinancialDocumentUrl(entry.url_viewer ?? entry.urlViewer ?? null);
    const downloadUrl = normalizeFinancialDocumentUrl(entry.url_download ?? entry.urlDownload ?? null);

    const dedupeKey = `${source || ''}|${primaryUrl}|${referenceDate || ''}|${deliveryDate || ''}|${title || ''}`;
    if (dedupe.has(dedupeKey)) continue;
    dedupe.add(dedupeKey);

    rows.push({
      id,
      source,
      title,
      category,
      documentType,
      referenceDate,
      deliveryDate,
      status,
      url: primaryUrl,
      viewerUrl,
      downloadUrl,
    });
  }

  return rows.sort((left, right) => {
    const leftDate = left.referenceDate || left.deliveryDate || '';
    const rightDate = right.referenceDate || right.deliveryDate || '';
    return rightDate.localeCompare(leftDate);
  });
};

const AssetDetailsPage = () => {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  const { assetId = '' } = useParams();
  const [searchParams] = useSearchParams();
  const portfolioIdFromQuery = searchParams.get('portfolioId')?.trim() || '';

  const {
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    assets,
    transactions,
    loading,
    metrics,
  } = usePortfolioData();
  const portfolioId = selectedPortfolio;
  const portfolioMarketValueByAssetId = useMemo(() => metrics?.marketValues || {}, [metrics]);
  const [activeTab, setActiveTab] = useState<AssetDetailsTab>('overview');
  const [portfolioSearchTerm, setPortfolioSearchTerm] = useState('');
  const [portfolioItemsPerPage, setPortfolioItemsPerPage] = useState(5);
  const [currentQuote, setCurrentQuote] = useState<number | null>(null);
  const [averageCost, setAverageCost] = useState<number | null>(null);
  const [marketSeries, setMarketSeries] = useState<AssetPriceSeriesPoint[]>([]);
  const [assetSplitEvents, setAssetSplitEvents] = useState<AssetSplitEvent[]>([]);
  const [marketSeriesLoading, setMarketSeriesLoading] = useState(false);
  const [hoveredMarketPointIndex, setHoveredMarketPointIndex] = useState<number | null>(null);
  const [selectedTradePoint, setSelectedTradePoint] = useState<AssetTradeHistoryPoint | null>(null);
  const [chartPeriod, setChartPeriod] = useState<ChartPeriodPreset>('MAX');
  const [customRangeStart, setCustomRangeStart] = useState('');
  const [customRangeEnd, setCustomRangeEnd] = useState('');
  const [assetInsights, setAssetInsights] = useState<AssetInsightsSnapshot | null>(null);
  const [assetFinancials, setAssetFinancials] = useState<AssetFinancialStatements | null>(null);
  const [assetFinancialsLoading, setAssetFinancialsLoading] = useState(false);
  const [assetFinancialsError, setAssetFinancialsError] = useState<string | null>(null);
  const [selectedFinancialStatement, setSelectedFinancialStatement] = useState<FinancialStatementKind>('income');
  const [selectedFinancialFrequency, setSelectedFinancialFrequency] = useState<FinancialFrequency>('annual');
  const [selectedFinancialMetric, setSelectedFinancialMetric] = useState('');
  const [selectedDocumentCategory, setSelectedDocumentCategory] = useState('all');
  const [selectedDocumentType, setSelectedDocumentType] = useState('all');
  const [newsTilesPerRow, setNewsTilesPerRow] = useState<number>(() => {
    if (typeof window === 'undefined') return NEWS_TILES_DEFAULT;
    try {
      const stored = window.localStorage.getItem(NEWS_TILES_STORAGE_KEY);
      return clampNewsTilesPerRow(stored);
    } catch {
      return NEWS_TILES_DEFAULT;
    }
  });
  const [newsLayoutTransitionKey, setNewsLayoutTransitionKey] = useState(0);
  const [assetNews, setAssetNews] = useState<AssetNewsItem[]>([]);
  const [assetNewsLoading, setAssetNewsLoading] = useState(false);
  const [assetNewsError, setAssetNewsError] = useState<string | null>(null);
  const [fiiUpdates, setFiiUpdates] = useState<Array<{
    id: number;
    category: string | null;
    title: string;
    deliveryDate: string | null;
    referenceDate: string | null;
    url: string;
    source: string;
  }>>([]);
  const [fiiUpdatesLoading, setFiiUpdatesLoading] = useState(false);
  const [fiiEmissions, setFiiEmissions] = useState<FiiEmissionRow[]>([]);
  const [fiiEmissionsLoading, setFiiEmissionsLoading] = useState(false);
  const [emissionsSearchTerm, setEmissionsSearchTerm] = useState('');
  const [emissionsItemsPerPage, setEmissionsItemsPerPage] = useState(5);
  const [portfolioCityCoordinates, setPortfolioCityCoordinates] = useState<Record<string, PortfolioCityCoordinate | null>>({});
  const [portfolioCityLoading, setPortfolioCityLoading] = useState(false);
  const [portfolioMapError, setPortfolioMapError] = useState<string | null>(null);
  const portfolioMapContainerRef = useRef<HTMLDivElement | null>(null);
  const portfolioLeafletContainerRef = useRef<HTMLDivElement | null>(null);
  const portfolioLeafletMapRef = useRef<LeafletMap | null>(null);
  const portfolioLeafletLayerRef = useRef<LeafletLayerGroup | null>(null);
  const portfolioLeafletScaleControlRef = useRef<LeafletScaleControl | null>(null);
  const newsLayoutInitializedRef = useRef(false);

  const numberLocale = i18n.language?.startsWith('pt') ? 'pt-BR' : 'en-US';

  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem(NEWS_TILES_STORAGE_KEY, String(newsTilesPerRow));
    } catch {
      // Ignore storage failures (private browsing / blocked storage).
    }
  }, [newsTilesPerRow]);

  useEffect(() => {
    if (!newsLayoutInitializedRef.current) {
      newsLayoutInitializedRef.current = true;
      return;
    }
    setNewsLayoutTransitionKey((previous) => previous + 1);
  }, [newsTilesPerRow]);

  const formatDetailValue = useCallback((value: unknown) => {
    if (value === undefined || value === null || value === '') return t('assets.modal.noValue');
    return String(value);
  }, [t]);

  const formatCnpjValue = useCallback((value: string | null) => {
    if (!value) return formatDetailValue(null);
    const digits = value.replace(/\D/g, '');
    if (digits.length !== 14) return value;
    return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12, 14)}`;
  }, [formatDetailValue]);

  const formatAssetQuantity = useCallback((value: unknown) => {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return formatDetailValue(value);

    const hasFraction = Math.abs(numeric % 1) > Number.EPSILON;
    return numeric.toLocaleString(numberLocale, {
      minimumFractionDigits: hasFraction ? DECIMAL_PRECISION : 0,
      maximumFractionDigits: hasFraction ? DECIMAL_PRECISION : 0,
    });
  }, [formatDetailValue, numberLocale]);

  const formatSignedCurrency = useCallback((value: number, currency: string) => {
    const absolute = formatCurrency(Math.abs(value), currency, numberLocale);
    if (Math.abs(value) <= Number.EPSILON) return absolute;
    return `${value > 0 ? '+' : '-'}${absolute}`;
  }, [numberLocale]);

  const formatSignedPercent = useCallback((value: number) => {
    const absolute = Math.abs(value).toLocaleString(numberLocale, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
    return `${value >= 0 ? '+' : '-'}${absolute}%`;
  }, [numberLocale]);

  const formatPercent = useCallback((ratio: number) => (
    `${(ratio * 100).toLocaleString(numberLocale, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}%`
  ), [numberLocale]);

  const formatSplitFactor = useCallback((factor: number) => (
    factor.toLocaleString(numberLocale, {
      minimumFractionDigits: Number.isInteger(factor) ? 0 : 2,
      maximumFractionDigits: 6,
    })
  ), [numberLocale]);

  const formatSplitRatio = useCallback((factor: number) => {
    if (!Number.isFinite(factor) || factor <= 0 || Math.abs(factor - 1) <= Number.EPSILON) return '-';
    if (factor > 1) {
      return `1:${formatSplitFactor(factor)}`;
    }
    return `${formatSplitFactor(1 / factor)}:1`;
  }, [formatSplitFactor]);

  const formatCompactNumber = useCallback((value: number) => (
    value.toLocaleString(numberLocale, {
      maximumFractionDigits: 2,
      notation: 'compact',
      compactDisplay: 'short',
    })
  ), [numberLocale]);

  const formatCountryDetail = useCallback((country: string) =>
    `${COUNTRY_FLAG_MAP[country] || '🏳️'} ${COUNTRY_NAME_MAP[country] || country}`, []);

  const formatNewsPublishedAt = useCallback((value: string | null) => {
    if (!value) return formatDetailValue(null);
    const parsed = new Date(value);
    if (!Number.isFinite(parsed.getTime())) return value;
    return parsed.toLocaleString(numberLocale, {
      year: 'numeric',
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
  }, [formatDetailValue, numberLocale]);

  // Sync portfolio selection from URL query if present.
  useEffect(() => {
    if (portfolioIdFromQuery && portfolios.length > 0) {
      const match = portfolios.find((item) => item.portfolioId === portfolioIdFromQuery);
      if (match && match.portfolioId !== selectedPortfolio) {
        setSelectedPortfolio(match.portfolioId);
      }
    }
  }, [portfolioIdFromQuery, portfolios, selectedPortfolio, setSelectedPortfolio]);

  const hasPrimaryTradeByAssetId = useMemo(() => {
    const set = new Set<string>();

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      if (normalizedType !== 'buy' && normalizedType !== 'sell' && normalizedType !== 'subscription') {
        continue;
      }

      const sourceTag = normalizeText(transaction.sourceDocId);
      if (!sourceTag.includes('B3-NEGOCIACAO')) continue;
      set.add(transaction.assetId);
    }

    return set;
  }, [transactions]);

  const shouldIgnoreConsolidatedTrade = useCallback((transaction: Transaction) => {
    const normalizedType = transaction.type?.toLowerCase() || '';
    if (normalizedType !== 'buy' && normalizedType !== 'sell' && normalizedType !== 'subscription') {
      return false;
    }

    const sourceTag = normalizeText(transaction.sourceDocId);
    if (!sourceTag.includes('B3-RELATORIO')) return false;

    return hasPrimaryTradeByAssetId.has(transaction.assetId);
  }, [hasPrimaryTradeByAssetId]);

  const assetQuantitiesById = useMemo(() => {
    const quantities: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;
      if (shouldIgnoreConsolidatedTrade(transaction)) continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      const normalizedQuantity = Math.round(Number(transaction.quantity || 0) * DECIMAL_FACTOR) / DECIMAL_FACTOR;
      if (!Number.isFinite(normalizedQuantity)) continue;

      if (normalizedType === 'buy' || normalizedType === 'subscription') {
        quantities[transaction.assetId] = (quantities[transaction.assetId] || 0) + normalizedQuantity;
        continue;
      }

      if (normalizedType === 'sell') {
        quantities[transaction.assetId] = (quantities[transaction.assetId] || 0) - normalizedQuantity;
      }
    }

    return quantities;
  }, [shouldIgnoreConsolidatedTrade, transactions]);

  const assetInvestedAmountById = useMemo(() => {
    const investedById: Record<string, number> = {};

    for (const transaction of transactions) {
      const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
      if (normalizedStatus !== 'confirmed') continue;
      if (shouldIgnoreConsolidatedTrade(transaction)) continue;

      const amount = Number(transaction.amount || 0);
      if (!Number.isFinite(amount)) continue;

      const normalizedType = transaction.type?.toLowerCase() || '';
      if (normalizedType === 'buy' || normalizedType === 'subscription') {
        investedById[transaction.assetId] = (investedById[transaction.assetId] || 0) + amount;
        continue;
      }

      if (normalizedType === 'sell') {
        investedById[transaction.assetId] = (investedById[transaction.assetId] || 0) - amount;
      }
    }

    return investedById;
  }, [shouldIgnoreConsolidatedTrade, transactions]);

  const assetSourcesById = useMemo(() => {
    const sources: Record<string, string[]> = {};

    for (const transaction of transactions) {
      const sourceDocId = transaction.sourceDocId?.toString().trim();
      const institution = transaction.institution?.toString().trim();

      if (sourceDocId) {
        sources[transaction.assetId] = [...(sources[transaction.assetId] || []), sourceDocId];
      }

      if (institution) {
        sources[transaction.assetId] = [...(sources[transaction.assetId] || []), institution];
      }
    }

    return sources;
  }, [transactions]);

  const selectedAsset = useMemo<AssetRow | null>(() => {
    const baseAsset = assets.find((entry) => entry.assetId === assetId);
    if (!baseAsset) return null;

    const labels = new Set<string>();
    const assetSource = summarizeSourceValue(baseAsset.source);
    if (assetSource) labels.add(assetSource);

    for (const candidate of (assetSourcesById[baseAsset.assetId] || [])) {
      const label = summarizeSourceValue(candidate);
      if (label) labels.add(label);
    }

    return {
      ...baseAsset,
      quantity: Number.isFinite(Number(baseAsset.quantity))
        ? Number(baseAsset.quantity)
        : (assetQuantitiesById[baseAsset.assetId] || 0),
      source: labels.size > 0 ? Array.from(labels).join(', ') : null,
      investedAmount: assetInvestedAmountById[baseAsset.assetId] || 0,
    };
  }, [assetId, assetInvestedAmountById, assetQuantitiesById, assetSourcesById, assets]);

  const buildInsightsSnapshot = useCallback((asset: AssetRow, detailsPayload: unknown, fairPayload: unknown): AssetInsightsSnapshot => {
    const details = toObjectRecord(detailsPayload);
    const fair = toObjectRecord(fairPayload);
    const detail = toObjectRecord(details.detail);
    const quote = toObjectRecord(detail.quote);
    const fundamentals = toObjectRecord(detail.fundamentals);
    const raw = toObjectRecord(detail.raw);
    const finalPayload = toObjectRecord(raw.final_payload);
    const primaryPayload = toObjectRecord(raw.primary_payload);
    const finalInfo = toObjectRecord(finalPayload.info);
    const primaryInfo = toObjectRecord(primaryPayload.info);
    const latestPrice = toObjectRecord(details.latest_price);
    const fairFundamentals = toObjectRecord(fair.fundamentals);

    return {
      ...createEmptyInsightsSnapshot('ready', asset.ticker, asset.assetClass),
      status: 'ready',
      source: toNonEmptyString(detail.data_source) || toNonEmptyString(latestPrice.source),
      fetchedAt: toNonEmptyString(details.fetched_at) || toNonEmptyString(detail.fetched_at) || toNonEmptyString(fair.fetched_at),
      sector: toNonEmptyString(finalInfo.sector) || toNonEmptyString(primaryInfo.sector) || toNonEmptyString(fundamentals.sector),
      industry: toNonEmptyString(finalInfo.industry)
        || toNonEmptyString(finalInfo.segment)
        || toNonEmptyString(primaryInfo.industry)
        || toNonEmptyString(primaryInfo.segment)
        || toNonEmptyString(fundamentals.industry),
      marketCap: firstFiniteNumber(finalInfo.marketCap, primaryInfo.marketCap, quote.marketCap, latestPrice.marketCap),
      averageVolume: firstFiniteNumber(
        finalInfo.averageVolume,
        finalInfo.averageDailyVolume10Day,
        primaryInfo.averageVolume,
        primaryInfo.averageDailyVolume10Day,
        quote.volume,
        latestPrice.volume,
      ),
      currentPrice: firstFiniteNumber(fair.current_price, latestPrice.close, quote.currentPrice, asset.currentPrice),
      graham: firstFiniteNumber(fair.graham),
      bazin: firstFiniteNumber(fair.bazin),
      fairPrice: firstFiniteNumber(fair.fair_price),
      marginOfSafetyPct: firstFiniteNumber(fair.margin_of_safety_pct),
      pe: firstFiniteNumber(
        fairFundamentals.pe,
        finalInfo.trailingPE,
        finalInfo.pe,
        primaryInfo.trailingPE,
        primaryInfo.pe,
        fundamentals.pe,
      ),
      pb: firstFiniteNumber(
        fairFundamentals.pb,
        finalInfo.priceToBook,
        finalInfo.pvp,
        primaryInfo.priceToBook,
        primaryInfo.pvp,
        fundamentals.pb,
      ),
      roe: firstFiniteNumber(
        fairFundamentals.roe,
        finalInfo.returnOnEquity,
        finalInfo.roe,
        primaryInfo.returnOnEquity,
        primaryInfo.roe,
        fundamentals.roe,
      ),
      roa: firstFiniteNumber(
        fairFundamentals.roa,
        finalInfo.returnOnAssets,
        finalInfo.roa,
        primaryInfo.returnOnAssets,
        primaryInfo.roa,
        fundamentals.roa,
      ),
      roic: firstFiniteNumber(
        fairFundamentals.roic,
        finalInfo.returnOnInvestedCapital,
        finalInfo.roic,
        primaryInfo.returnOnInvestedCapital,
        primaryInfo.roic,
        fundamentals.roic,
      ),
      payout: firstFiniteNumber(
        fairFundamentals.payout,
        finalInfo.payoutRatio,
        finalInfo.payout,
        primaryInfo.payoutRatio,
        primaryInfo.payout,
        fundamentals.payout,
      ),
      evEbitda: firstFiniteNumber(
        fairFundamentals.evEbitda,
        finalInfo.enterpriseToEbitda,
        primaryInfo.enterpriseToEbitda,
        fundamentals.evEbitda,
      ),
      netDebtEbitda: firstFiniteNumber(
        fairFundamentals.netDebtEbitda,
        finalInfo.netDebtToEbitda,
        finalInfo.netDebtEbitda,
        primaryInfo.netDebtToEbitda,
        primaryInfo.netDebtEbitda,
        fundamentals.netDebtEbitda,
      ),
      lpa: firstFiniteNumber(
        fairFundamentals.lpa,
        finalInfo.trailingEps,
        finalInfo.epsTrailingTwelveMonths,
        finalInfo.lpa,
        primaryInfo.trailingEps,
        primaryInfo.epsTrailingTwelveMonths,
        primaryInfo.lpa,
        fundamentals.lpa,
      ),
      vpa: firstFiniteNumber(
        fairFundamentals.vpa,
        finalInfo.bookValue,
        finalInfo.vpa,
        primaryInfo.bookValue,
        primaryInfo.vpa,
        fundamentals.vpa,
      ),
      netMargin: firstFiniteNumber(
        fairFundamentals.netMargin,
        finalInfo.profitMargins,
        finalInfo.netMargin,
        primaryInfo.profitMargins,
        primaryInfo.netMargin,
        fundamentals.netMargin,
      ),
      ebitMargin: firstFiniteNumber(
        fairFundamentals.ebitMargin,
        finalInfo.operatingMargins,
        finalInfo.ebitMargin,
        primaryInfo.operatingMargins,
        primaryInfo.ebitMargin,
        fundamentals.ebitMargin,
      ),
      errorMessage: null,
    };
  }, []);

  const assetRows = useMemo<AssetRow[]>(() => {
    return assets.map((asset) => {
      const labels = new Set<string>();
      const assetSource = summarizeSourceValue(asset.source);
      if (assetSource) labels.add(assetSource);
      for (const candidate of (assetSourcesById[asset.assetId] || [])) {
        const label = summarizeSourceValue(candidate);
        if (label) labels.add(label);
      }

      return {
        ...asset,
        quantity: Number.isFinite(Number(asset.quantity))
          ? Number(asset.quantity)
          : (assetQuantitiesById[asset.assetId] || 0),
        source: labels.size > 0 ? Array.from(labels).join(', ') : null,
        investedAmount: assetInvestedAmountById[asset.assetId] || 0,
      };
    });
  }, [assetInvestedAmountById, assetQuantitiesById, assetSourcesById, assets]);

  const resolveRowCurrentValue = useCallback((row: AssetRow): number | null => {
    const metricCurrentValue = portfolioMarketValueByAssetId[row.assetId];
    if (typeof metricCurrentValue === 'number' && Number.isFinite(metricCurrentValue)) {
      return metricCurrentValue;
    }

    const quantity = Number(row.quantity);
    const hasOpenPosition = Number.isFinite(quantity) && Math.abs(quantity) > Number.EPSILON;
    const directCurrentPrice = Number(row.currentPrice);
    if (
      Number.isFinite(directCurrentPrice)
      && Number.isFinite(quantity)
      && (!hasOpenPosition || Math.abs(directCurrentPrice) > Number.EPSILON)
    ) {
      return directCurrentPrice * quantity;
    }

    const directCurrentValue = Number(row.currentValue);
    if (
      Number.isFinite(directCurrentValue)
      && (!hasOpenPosition || Math.abs(directCurrentValue) > Number.EPSILON)
    ) {
      return directCurrentValue;
    }

    return null;
  }, [portfolioMarketValueByAssetId]);

  const currentValueByAssetId = useMemo(() => {
    const values: Record<string, number | null> = {};
    for (const row of assetRows) {
      values[row.assetId] = resolveRowCurrentValue(row);
    }
    return values;
  }, [assetRows, resolveRowCurrentValue]);

  const portfolioCurrentTotal = useMemo<number>(() => (
    Object.values(currentValueByAssetId).reduce<number>((sum, value) => (
      typeof value === 'number' && Number.isFinite(value) ? sum + value : sum
    ), 0)
  ), [currentValueByAssetId]);

  useEffect(() => {
    setCurrentQuote(null);
    setAverageCost(null);
    setMarketSeries([]);
    setAssetSplitEvents([]);
    setHoveredMarketPointIndex(null);
    setSelectedTradePoint(null);
    setChartPeriod('MAX');
    setCustomRangeStart('');
    setCustomRangeEnd('');
    setAssetFinancials(null);
    setAssetFinancialsError(null);
    setSelectedFinancialStatement('income');
    setSelectedFinancialFrequency('annual');
    setSelectedFinancialMetric('');
    setSelectedDocumentCategory('all');
    setSelectedDocumentType('all');
    setAssetNews([]);
    setAssetNewsError(null);
    if (selectedAsset && isStockAssetClass(selectedAsset.assetClass)) {
      setAssetInsights(createEmptyInsightsSnapshot('loading', selectedAsset.ticker, selectedAsset.assetClass));
    } else {
      setAssetInsights(null);
    }
  }, [selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    if (!isStockAssetClass(selectedAsset.assetClass)) {
      setAssetInsights(null);
      return;
    }
    let cancelled = false;

    const loadPayloads = async () => {
      const [details, fair] = await Promise.all([
        api.getAssetDetails(selectedAsset.ticker, portfolioId),
        api.getAssetFairPrice(selectedAsset.ticker, portfolioId),
      ]);
      return { details, fair };
    };

    loadPayloads()
      .then(async (initialPayloads) => {
        let detailsPayload = initialPayloads.details;
        let fairPayload = initialPayloads.fair;
        const detailsRecord = toObjectRecord(detailsPayload);

        if (detailsRecord.detail == null) {
          await api.refreshMarketData(portfolioId, selectedAsset.assetId).catch(() => null);
          try {
            const refreshedPayloads = await loadPayloads();
            detailsPayload = refreshedPayloads.details;
            fairPayload = refreshedPayloads.fair;
          } catch {
            // Keep initial payload when refresh fetch fails.
          }
        }

        if (cancelled) return;
        setAssetInsights(buildInsightsSnapshot(selectedAsset, detailsPayload, fairPayload));
      })
      .catch((error) => {
        if (cancelled) return;
        const fallback = createEmptyInsightsSnapshot('error', selectedAsset.ticker, selectedAsset.assetClass);
        fallback.errorMessage = error instanceof Error ? error.message : null;
        setAssetInsights(fallback);
      });

    return () => {
      cancelled = true;
    };
  }, [buildInsightsSnapshot, portfolioId, selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    let cancelled = false;
    setAssetFinancialsLoading(true);
    setAssetFinancialsError(null);

    api.getAssetFinancials(selectedAsset.ticker, portfolioId)
      .then((payload) => {
        if (cancelled) return;
        setAssetFinancials(toObjectRecord(payload) as AssetFinancialStatements);
      })
      .catch((error) => {
        if (cancelled) return;
        setAssetFinancials(null);
        setAssetFinancialsError(error instanceof Error ? error.message : null);
      })
      .finally(() => {
        if (cancelled) return;
        setAssetFinancialsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    let cancelled = false;
    setAssetNewsLoading(true);
    setAssetNewsError(null);

    api.getAssetNews(selectedAsset.ticker, portfolioId)
      .then((payload) => {
        if (cancelled) return;
        setAssetNews(parseAssetNewsPayload(payload));
      })
      .catch((error) => {
        if (cancelled) return;
        setAssetNews([]);
        setAssetNewsError(error instanceof Error ? error.message : null);
      })
      .finally(() => {
        if (cancelled) return;
        setAssetNewsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    const isFii = String(selectedAsset.assetClass || '').toLowerCase() === 'fii';
    if (!isFii) {
      setFiiUpdates([]);
      return;
    }
    let cancelled = false;
    setFiiUpdatesLoading(true);

    api.getFiiUpdates(selectedAsset.ticker, portfolioId)
      .then((payload) => {
        if (cancelled) return;
        const payloadRecord = toObjectRecord(payload);
        const items = Array.isArray(payloadRecord.items) ? payloadRecord.items : [];
        setFiiUpdates(items);
      })
      .catch(() => {
        if (cancelled) return;
        setFiiUpdates([]);
      })
      .finally(() => {
        if (cancelled) return;
        setFiiUpdatesLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    const isFii = String(selectedAsset.assetClass || '').toLowerCase() === 'fii';
    if (!isFii) {
      setFiiEmissions([]);
      return;
    }
    let cancelled = false;
    setFiiEmissionsLoading(true);

    api.getFiiEmissions(selectedAsset.ticker, portfolioId)
      .then((payload) => {
        if (cancelled) return;
        const payloadRecord = toObjectRecord(payload);
        const items = Array.isArray(payloadRecord.emissions) ? payloadRecord.emissions : [];
        setFiiEmissions(items);
      })
      .catch(() => {
        if (cancelled) return;
        setFiiEmissions([]);
      })
      .finally(() => {
        if (cancelled) return;
        setFiiEmissionsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    let cancelled = false;

    api.getPriceAtDate(portfolioId, selectedAsset.ticker, new Date().toISOString().slice(0, 10))
      .then((payload) => {
        if (cancelled) return;
        const close = Number((payload as { close?: unknown }).close);
        setCurrentQuote(Number.isFinite(close) ? close : null);
      })
      .catch(() => {
        if (cancelled) return;
        setCurrentQuote(null);
      });

    api.getAverageCost(portfolioId, selectedAsset.ticker)
      .then((payload) => {
        if (cancelled) return;
        const parsedAverageCost = Number((payload as { average_cost?: unknown }).average_cost);
        setAverageCost(Number.isFinite(parsedAverageCost) ? parsedAverageCost : null);
      })
      .catch(() => {
        if (cancelled) return;
        setAverageCost(null);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  useEffect(() => {
    if (!selectedAsset || !portfolioId) return;
    let cancelled = false;
    setMarketSeriesLoading(true);

    api.getPriceChart(portfolioId, selectedAsset.ticker, 'price_history', 'MAX')
      .then((payload) => {
        if (cancelled) return;

        const chartPayload = payload as {
          series?: unknown[];
          split_events?: unknown[];
        };

        const normalizedSeries = Array.isArray(chartPayload.series)
          ? (chartPayload.series
            .map((item) => {
              const point = item as Record<string, unknown>;
              const close = Number(point.close);
              const stockSplits = Number(point.stock_splits ?? point.stockSplits);
              return {
                date: String(point.date || ''),
                display_date: point.display_date ? String(point.display_date) : undefined,
                close: Number.isFinite(close) ? close : null,
                stock_splits: Number.isFinite(stockSplits) ? stockSplits : null,
              } satisfies AssetPriceSeriesPoint;
            })
            .filter((point) => point.date))
          : [];

        const splitEventsFromPayload = Array.isArray(chartPayload.split_events)
          ? chartPayload.split_events
            .map((item) => {
              const event = item as Record<string, unknown>;
              const dateCandidate = String(event.date || '').slice(0, 10);
              const normalizedDate = toIsoDate(dateCandidate);
              const factor = toNumericValue(event.factor ?? event.stock_splits ?? event.stockSplits);
              if (!normalizedDate || factor === null || factor <= 0 || Math.abs(factor - 1) <= Number.EPSILON) {
                return null;
              }
              return {
                date: normalizedDate,
                displayDate: String(event.display_date || event.displayDate || normalizedDate),
                factor,
                eventType: normalizeSplitEventType(event.event_type ?? event.type, factor),
              } satisfies AssetSplitEvent;
            })
            .filter((event): event is AssetSplitEvent => Boolean(event))
          : [];

        const splitEventsFromSeries = normalizedSeries
          .filter((point) => (
            point.stock_splits !== null
            && point.stock_splits > 0
            && Math.abs(point.stock_splits - 1) > Number.EPSILON
          ))
          .map((point) => ({
            date: point.date,
            displayDate: point.display_date || point.date,
            factor: Number(point.stock_splits),
            eventType: normalizeSplitEventType(null, Number(point.stock_splits)),
          } satisfies AssetSplitEvent));

        const normalizedSplitEvents = dedupeSplitEvents(
          splitEventsFromPayload.length > 0 ? splitEventsFromPayload : splitEventsFromSeries
        );

        setMarketSeries(normalizedSeries);
        setAssetSplitEvents(normalizedSplitEvents);
      })
      .catch(() => {
        if (cancelled) return;
        setMarketSeries([]);
        setAssetSplitEvents([]);
      })
      .finally(() => {
        if (cancelled) return;
        setMarketSeriesLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioId, selectedAsset]);

  const fallbackAverageCost = useMemo(() => {
    if (!selectedAsset) return null;
    const quantity = Number(selectedAsset.quantity);
    const investedAmount = Number(selectedAsset.investedAmount);
    if (!Number.isFinite(quantity) || !Number.isFinite(investedAmount)) return null;
    if (Math.abs(quantity) <= Number.EPSILON) return null;
    return investedAmount / quantity;
  }, [selectedAsset]);

  const resolvedAverageCost = useMemo(() => {
    if (typeof averageCost === 'number' && Number.isFinite(averageCost)) return averageCost;
    if (selectedAsset) {
      const cached = metrics?.averageCosts?.[selectedAsset.assetId];
      if (typeof cached === 'number' && Number.isFinite(cached)) return cached;
    }
    return fallbackAverageCost;
  }, [averageCost, fallbackAverageCost, metrics, selectedAsset]);

  const resolvedCurrentPrice = useMemo(() => {
    if (!selectedAsset) return null;
    if (typeof currentQuote === 'number' && Number.isFinite(currentQuote)) return currentQuote;

    const quantity = Number(selectedAsset.quantity);
    const hasOpenPosition = Number.isFinite(quantity) && Math.abs(quantity) > Number.EPSILON;
    const insightQuote = assetInsights?.status === 'ready'
      ? toNumericValue(assetInsights.currentPrice)
      : null;
    if (
      insightQuote !== null
      && (!hasOpenPosition || Math.abs(insightQuote) > Number.EPSILON)
    ) {
      return insightQuote;
    }

    const cachedQuote = metrics?.currentQuotes?.[selectedAsset.assetId];
    if (
      typeof cachedQuote === 'number'
      && Number.isFinite(cachedQuote)
      && (!hasOpenPosition || Math.abs(cachedQuote) > Number.EPSILON)
    ) {
      return cachedQuote;
    }

    const directCurrentValue = Number(selectedAsset.currentValue);
    if (!Number.isFinite(directCurrentValue) || !Number.isFinite(quantity)) return null;
    if (Math.abs(quantity) <= Number.EPSILON) return null;
    return directCurrentValue / quantity;
  }, [assetInsights, currentQuote, metrics, selectedAsset]);

  const resolvedCurrentValue = useMemo(() => {
    if (!selectedAsset) return null;
    const metricCurrentValue = portfolioMarketValueByAssetId[selectedAsset.assetId];
    if (typeof metricCurrentValue === 'number' && Number.isFinite(metricCurrentValue)) {
      return metricCurrentValue;
    }

    const quantity = Number(selectedAsset.quantity);
    if (resolvedCurrentPrice !== null && Number.isFinite(quantity)) {
      return quantity * resolvedCurrentPrice;
    }

    const directCurrentValue = Number(selectedAsset.currentValue);
    return Number.isFinite(directCurrentValue) ? directCurrentValue : null;
  }, [portfolioMarketValueByAssetId, resolvedCurrentPrice, selectedAsset]);

  const quoteVsAverage = useMemo(() => {
    if (resolvedCurrentPrice === null || resolvedAverageCost === null) return null;
    return resolvedCurrentPrice - resolvedAverageCost;
  }, [resolvedAverageCost, resolvedCurrentPrice]);

  const balanceMinusInvested = useMemo(() => {
    if (!selectedAsset || resolvedCurrentValue === null) return null;
    const investedAmount = Number(selectedAsset.investedAmount);
    if (!Number.isFinite(investedAmount)) return null;
    return resolvedCurrentValue - investedAmount;
  }, [resolvedCurrentValue, selectedAsset]);

  const positionStatus = useMemo(() => {
    if (balanceMinusInvested === null) return null;
    if (Math.abs(balanceMinusInvested) <= Number.EPSILON) return 'neutral';
    return balanceMinusInvested > 0 ? 'positive' : 'negative';
  }, [balanceMinusInvested]);

  const selectedAssetWeightMetrics = useMemo(() => {
    if (!selectedAsset) return null;

    const storedSelectedCurrentValue = currentValueByAssetId[selectedAsset.assetId] ?? 0;
    const selectedCurrentValue = resolvedCurrentValue ?? storedSelectedCurrentValue;
    const adjustedPortfolioTotal = portfolioCurrentTotal - storedSelectedCurrentValue + selectedCurrentValue;
    const portfolioWeight = adjustedPortfolioTotal > 0 ? selectedCurrentValue / adjustedPortfolioTotal : 0;

    const storedClassTotal = assetRows
      .filter((row) => row.assetClass === selectedAsset.assetClass)
      .reduce((sum, row) => {
        const value = currentValueByAssetId[row.assetId];
        return typeof value === 'number' && Number.isFinite(value) ? sum + value : sum;
      }, 0);
    const adjustedClassTotal = storedClassTotal - storedSelectedCurrentValue + selectedCurrentValue;
    const classWeight = adjustedClassTotal > 0 ? selectedCurrentValue / adjustedClassTotal : 0;

    return {
      selectedCurrentValue,
      portfolioTotal: adjustedPortfolioTotal,
      portfolioWeight,
      classTotal: adjustedClassTotal,
      classWeight,
    };
  }, [assetRows, currentValueByAssetId, portfolioCurrentTotal, resolvedCurrentValue, selectedAsset]);

  const assetTradeHistoryRows = useMemo<AssetTradeHistoryRow[]>(() => {
    if (!selectedAsset) return [];

    return transactions
      .filter((transaction) => {
        if (transaction.assetId !== selectedAsset.assetId) return false;
        const normalizedStatus = transaction.status?.toLowerCase() || 'unknown';
        if (normalizedStatus !== 'confirmed') return false;
        if (shouldIgnoreConsolidatedTrade(transaction)) return false;
        const normalizedType = transaction.type?.toLowerCase() || '';
        return normalizedType === 'buy' || normalizedType === 'sell';
      })
      .map((transaction) => ({
        transId: transaction.transId,
        date: transaction.date || transaction.createdAt?.slice(0, 10) || '',
        type: transaction.type.toLowerCase() as 'buy' | 'sell',
        quantity: Number(transaction.quantity || 0),
        price: Number(transaction.price || 0),
        amount: Number(transaction.amount || 0),
        currency: transaction.currency || selectedAsset.currency || 'BRL',
        source: summarizeSourceValue(transaction.sourceDocId || transaction.institution) || null,
      }))
      .filter((row) => row.date && Number.isFinite(row.price))
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime());
  }, [selectedAsset, shouldIgnoreConsolidatedTrade, transactions]);

  const todayIso = useMemo(() => new Date().toISOString().slice(0, 10), []);
  const firstTradeDate = useMemo(() => (
    assetTradeHistoryRows[0]?.date || null
  ), [assetTradeHistoryRows]);
  const firstSeriesDate = useMemo(() => {
    const sorted = [...marketSeries]
      .filter((row) => row.date)
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime());
    return sorted[0]?.date || null;
  }, [marketSeries]);
  const minSelectableStartDate = firstTradeDate || firstSeriesDate;

  useEffect(() => {
    if (!minSelectableStartDate) {
      setCustomRangeStart('');
      setCustomRangeEnd(todayIso);
      return;
    }

    setCustomRangeStart((previous) => {
      const normalizedPrevious = toIsoDate(previous);
      if (!normalizedPrevious) return minSelectableStartDate;
      if (normalizedPrevious < minSelectableStartDate) return minSelectableStartDate;
      if (normalizedPrevious > todayIso) return todayIso;
      return normalizedPrevious;
    });

    setCustomRangeEnd((previous) => {
      const normalizedPrevious = toIsoDate(previous);
      if (!normalizedPrevious) return todayIso;
      if (normalizedPrevious < minSelectableStartDate) return minSelectableStartDate;
      if (normalizedPrevious > todayIso) return todayIso;
      return normalizedPrevious;
    });
  }, [minSelectableStartDate, todayIso]);

  const effectiveChartRange = useMemo(() => {
    const minStart = minSelectableStartDate ? toIsoDate(minSelectableStartDate) : null;
    const maxEnd = toIsoDate(todayIso) || todayIso;

    if (chartPeriod === 'CUSTOM') {
      let start = toIsoDate(customRangeStart) || minStart;
      let end = toIsoDate(customRangeEnd) || maxEnd;

      if (minStart && start && start < minStart) start = minStart;
      if (end > maxEnd) end = maxEnd;
      if (start && start > end) start = end;

      return { start, end };
    }

    if (chartPeriod === 'MAX') {
      return { start: null, end: maxEnd };
    }

    const days = CHART_PERIOD_DAYS[chartPeriod];
    if (!Number.isFinite(days)) {
      return { start: null, end: maxEnd };
    }

    let start = addDaysToIsoDate(maxEnd, -Number(days));
    if (minStart && start < minStart) {
      start = minStart;
    }

    return { start, end: maxEnd };
  }, [chartPeriod, customRangeEnd, customRangeStart, minSelectableStartDate, todayIso]);

  const marketSeriesInRange = useMemo(() => (
    marketSeries
      .filter((row) => {
        if (!row.date) return false;
        if (effectiveChartRange.start && row.date < effectiveChartRange.start) return false;
        if (effectiveChartRange.end && row.date > effectiveChartRange.end) return false;
        return true;
      })
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime())
  ), [effectiveChartRange.end, effectiveChartRange.start, marketSeries]);

  const marketPriceChart = useMemo(() => {
    const pointsInput = marketSeriesInRange
      .filter((row) => row.date && Number.isFinite(row.close))
      .sort((left, right) => new Date(left.date).getTime() - new Date(right.date).getTime());
    if (!pointsInput.length) return null;

    const chartPadding = { top: 16, right: 20, bottom: 28, left: 20 };
    const chartWidth = HISTORY_CHART_WIDTH - chartPadding.left - chartPadding.right;
    const chartHeight = HISTORY_CHART_HEIGHT - chartPadding.top - chartPadding.bottom;
    const closes = pointsInput.map((row) => Number(row.close));
    const minClose = Math.min(...closes);
    const maxClose = Math.max(...closes);
    const spread = Math.max(maxClose - minClose, 0.01);
    const paddedMin = minClose - spread * 0.08;
    const paddedMax = maxClose + spread * 0.08;
    const yBase = chartPadding.top + chartHeight;

    const xFor = (index: number) => (
      pointsInput.length === 1
        ? chartPadding.left + chartWidth / 2
        : chartPadding.left + (index / (pointsInput.length - 1)) * chartWidth
    );
    const yFor = (close: number) => (
      chartPadding.top + (1 - (close - paddedMin) / (paddedMax - paddedMin)) * chartHeight
    );

    const points = pointsInput.map((row, index) => {
      const close = Number(row.close);
      const previousClose = index > 0 ? Number(pointsInput[index - 1].close) : null;
      const change = previousClose !== null ? close - previousClose : null;
      const changePct =
        previousClose !== null && Math.abs(previousClose) > Number.EPSILON
          ? (change! / previousClose) * 100
          : null;
      return {
        date: row.date,
        displayDate: row.display_date || row.date,
        close,
        change,
        changePct,
        index,
        x: xFor(index),
        y: yFor(close),
      };
    });

    const polyline = points
      .map((point, index) => {
        const x = point.x.toFixed(2);
        const y = point.y.toFixed(2);
        return `${index === 0 ? 'M' : 'L'} ${x} ${y}`;
      })
      .join(' ');
    const areaPath = `${polyline} L ${points[points.length - 1].x.toFixed(2)} ${yBase.toFixed(2)} L ${points[0].x.toFixed(2)} ${yBase.toFixed(2)} Z`;

    return {
      points,
      polyline,
      areaPath,
      firstDate: points[0].date,
      lastDate: points[points.length - 1].date,
      lastClose: points[points.length - 1].close,
      minClose: paddedMin,
      maxClose: paddedMax,
      padding: chartPadding,
      yBase,
    };
  }, [marketSeriesInRange]);

  const hoveredMarketPoint = useMemo(() => {
    if (!marketPriceChart || hoveredMarketPointIndex === null) return null;
    return marketPriceChart.points[hoveredMarketPointIndex] || null;
  }, [hoveredMarketPointIndex, marketPriceChart]);

  const hoveredMarketTooltipStyle = useMemo(() => {
    if (!hoveredMarketPoint) return null;
    const isRightSide = hoveredMarketPoint.x > HISTORY_CHART_WIDTH * 0.68;
    const isNearTop = hoveredMarketPoint.y < HISTORY_CHART_HEIGHT * 0.25;
    return {
      left: `${(hoveredMarketPoint.x / HISTORY_CHART_WIDTH) * 100}%`,
      top: `${(hoveredMarketPoint.y / HISTORY_CHART_HEIGHT) * 100}%`,
      transform: `translate(${isRightSide ? '-100%' : '0'}, ${isNearTop ? '12px' : 'calc(-100% - 12px)'})`,
    };
  }, [hoveredMarketPoint]);

  const assetTradeHistoryStats = useMemo(() => {
    const buys = assetTradeHistoryRows.filter((row) => row.type === 'buy');
    const sells = assetTradeHistoryRows.filter((row) => row.type === 'sell');

    const weightedAveragePrice = (rows: AssetTradeHistoryRow[]) => {
      const totalQuantity = rows.reduce((sum, row) => sum + row.quantity, 0);
      if (Math.abs(totalQuantity) <= Number.EPSILON) return null;
      const totalAmount = rows.reduce((sum, row) => sum + (row.price * row.quantity), 0);
      return totalAmount / totalQuantity;
    };

    return {
      trades: assetTradeHistoryRows.length,
      buys: buys.length,
      sells: sells.length,
      avgBuyPrice: weightedAveragePrice(buys),
      avgSellPrice: weightedAveragePrice(sells),
    };
  }, [assetTradeHistoryRows]);

  const assetTradeHistoryChart = useMemo(() => {
    const rows = assetTradeHistoryRows.filter((row) => Number.isFinite(row.price));
    if (!rows.length) return null;

    const chartPadding = { top: 16, right: 20, bottom: 28, left: 20 };
    const chartWidth = HISTORY_CHART_WIDTH - chartPadding.left - chartPadding.right;
    const chartHeight = HISTORY_CHART_HEIGHT - chartPadding.top - chartPadding.bottom;
    const prices = rows.map((point) => point.price);
    const minPrice = Math.min(...prices);
    const maxPrice = Math.max(...prices);
    const spread = Math.max(maxPrice - minPrice, 1);
    const paddedMin = minPrice - spread * 0.08;
    const paddedMax = maxPrice + spread * 0.08;
    const yBase = chartPadding.top + chartHeight;

    const xFor = (index: number) =>
      rows.length === 1
        ? chartPadding.left + chartWidth / 2
        : chartPadding.left + (index / (rows.length - 1)) * chartWidth;
    const yFor = (price: number) =>
      chartPadding.top + (1 - (price - paddedMin) / (paddedMax - paddedMin)) * chartHeight;

    const points: AssetTradeHistoryPoint[] = rows.map((point, index) => ({
      ...point,
      x: xFor(index),
      y: yFor(point.price),
      index,
    }));

    const polyline = points
      .map((point, index) => {
        const x = point.x.toFixed(2);
        const y = point.y.toFixed(2);
        return `${index === 0 ? 'M' : 'L'} ${x} ${y}`;
      })
      .join(' ');
    const areaPath = `${polyline} L ${points[points.length - 1].x.toFixed(2)} ${yBase.toFixed(2)} L ${points[0].x.toFixed(2)} ${yBase.toFixed(2)} Z`;

    return {
      points,
      polyline,
      areaPath,
      firstDate: rows[0].date,
      lastDate: rows[rows.length - 1].date,
      lastPrice: rows[rows.length - 1].price,
      minPrice: paddedMin,
      maxPrice: paddedMax,
      padding: chartPadding,
      yBase,
    };
  }, [assetTradeHistoryRows]);

  const selectedTradeTooltipStyle = useMemo(() => {
    if (!selectedTradePoint) return null;
    const isRightSide = selectedTradePoint.x > HISTORY_CHART_WIDTH * 0.68;
    const isNearTop = selectedTradePoint.y < HISTORY_CHART_HEIGHT * 0.25;
    return {
      left: `${(selectedTradePoint.x / HISTORY_CHART_WIDTH) * 100}%`,
      top: `${(selectedTradePoint.y / HISTORY_CHART_HEIGHT) * 100}%`,
      transform: `translate(${isRightSide ? '-100%' : '0'}, ${isNearTop ? '12px' : 'calc(-100% - 12px)'})`,
    };
  }, [selectedTradePoint]);

  const splitEventsCoverage = useMemo(() => {
    if (assetSplitEvents.length === 0) return null;
    const ordered = [...assetSplitEvents].sort((left, right) => left.date.localeCompare(right.date));
    return {
      start: ordered[0].date,
      end: ordered[ordered.length - 1].date,
    };
  }, [assetSplitEvents]);

  const overviewFields = useMemo(() => {
    if (!selectedAsset) return [];

    return [
      {
        key: 'name',
        label: t('assets.modal.fields.name'),
        value: selectedAsset.name
          ? (
            <ExpandableText
              text={selectedAsset.name}
              maxLines={2}
              expandLabel={t('assets.modal.fields.nameExpandHint')}
              collapseLabel={t('assets.modal.fields.nameCollapseHint')}
            />
          )
          : formatDetailValue(selectedAsset.name),
      },
      { key: 'ticker', label: t('assets.modal.fields.ticker'), value: formatDetailValue(selectedAsset.ticker) },
      { key: 'quantity', label: t('assets.modal.fields.quantity'), value: formatAssetQuantity(selectedAsset.quantity) },
      {
        key: 'investedAmount',
        label: t('assets.modal.fields.investedAmount'),
        value: formatCurrency(selectedAsset.investedAmount, selectedAsset.currency || 'BRL', numberLocale),
      },
      {
        key: 'averagePrice',
        label: t('assets.modal.fields.averagePrice'),
        value: resolvedAverageCost !== null
          ? formatCurrency(resolvedAverageCost, selectedAsset.currency || 'BRL', numberLocale)
          : formatDetailValue(resolvedAverageCost),
      },
      {
        key: 'currentPrice',
        label: t('assets.modal.fields.currentPrice'),
        value: resolvedCurrentPrice !== null
          ? formatCurrency(resolvedCurrentPrice, selectedAsset.currency || 'BRL', numberLocale)
          : formatDetailValue(null),
      },
      {
        key: 'currentValue',
        label: t('assets.modal.fields.currentValue'),
        value: resolvedCurrentValue !== null
          ? formatCurrency(resolvedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)
          : formatDetailValue(selectedAsset.currentValue),
      },
    ];
  }, [formatAssetQuantity, formatDetailValue, numberLocale, resolvedAverageCost, resolvedCurrentPrice, resolvedCurrentValue, selectedAsset, t]);

  const marketFields = useMemo(() => {
    if (!selectedAsset) return [];

    return [
      {
        key: 'assetClass',
        label: t('assets.modal.fields.class'),
        value: t(`assets.classes.${selectedAsset.assetClass}`, { defaultValue: selectedAsset.assetClass }),
      },
      {
        key: 'status',
        label: t('assets.modal.fields.status'),
        value: t(`assets.statuses.${selectedAsset.status?.toLowerCase() || 'unknown'}`, {
          defaultValue: selectedAsset.status || t('assets.statuses.unknown'),
        }),
      },
      {
        key: 'country',
        label: t('assets.modal.fields.country'),
        value: formatCountryDetail(selectedAsset.country),
      },
      {
        key: 'currency',
        label: t('assets.modal.fields.currency'),
        value: formatDetailValue(selectedAsset.currency),
      },
      {
        key: 'quoteVsAverage',
        label: t('assets.modal.fields.quoteVsAverage'),
        value:
          quoteVsAverage !== null
            ? formatSignedCurrency(quoteVsAverage, selectedAsset.currency || 'BRL')
            : formatDetailValue(quoteVsAverage),
      },
      {
        key: 'investedMinusCurrent',
        label: t('assets.modal.fields.investedMinusCurrent'),
        value:
          balanceMinusInvested !== null
            ? formatSignedCurrency(balanceMinusInvested, selectedAsset.currency || 'BRL')
            : formatDetailValue(balanceMinusInvested),
      },
      {
        key: 'positionStatus',
        label: t('assets.modal.fields.positionStatus'),
        value:
          positionStatus
            ? (
              <span className={`assets-page__position assets-page__position--${positionStatus}`}>
                {t(`assets.modal.position.${positionStatus}`)}
              </span>
            )
            : formatDetailValue(positionStatus),
      },
    ];
  }, [balanceMinusInvested, formatCountryDetail, formatDetailValue, formatSignedCurrency, positionStatus, quoteVsAverage, selectedAsset, t]);

  const insightsLinks = useMemo(() => {
    if (!assetInsights || assetInsights.status !== 'ready') return [];

    const links = [
      {
        key: 'status-invest',
        label: t('assets.modal.insights.links.statusInvest', { defaultValue: 'Status Invest' }),
        href: assetInsights.statusInvestUrl,
      },
      {
        key: 'b3',
        label: t('assets.modal.insights.links.b3', { defaultValue: 'B3' }),
        href: assetInsights.b3Url,
      },
      {
        key: 'clube-fii',
        label: t('assets.modal.insights.links.clubeFii', { defaultValue: 'Clube FII' }),
        href: assetInsights.clubeFiiUrl,
      },
      {
        key: 'fiis',
        label: t('assets.modal.insights.links.fiis', { defaultValue: 'FIIs.com.br' }),
        href: assetInsights.fiisUrl,
      },
    ];

    return links.filter((entry): entry is { key: string; label: string; href: string } => Boolean(entry.href));
  }, [assetInsights, t]);

  const insightsGroups = useMemo(() => {
    if (!selectedAsset || !isStockAssetClass(selectedAsset.assetClass)) return [];

    const renderInsightsValue = (value: React.ReactNode) => {
      if (!assetInsights || assetInsights.status === 'loading') return t('common.loading');
      if (assetInsights.status === 'error') {
        return t('assets.modal.insights.unavailable', { defaultValue: 'Unavailable' });
      }
      return value;
    };

    const formatRatioAsPercent = (value: number | null, signed = false) => {
      const normalized = normalizeRatioMetric(value);
      if (normalized === null) return formatDetailValue(null);
      return signed
        ? formatSignedPercent(normalized * 100)
        : formatPercent(normalized);
    };

    const marginOfSafetyValue = (() => {
      if (!assetInsights || assetInsights.status !== 'ready') return renderInsightsValue(formatDetailValue(null));
      const ratio = normalizePercentValueToRatio(assetInsights.marginOfSafetyPct);
      if (ratio === null) return formatDetailValue(null);
      const trend = Math.abs(ratio) <= Number.EPSILON
        ? 'neutral'
        : ratio > 0
          ? 'positive'
          : 'negative';
      return (
        <span className={`assets-page__delta assets-page__delta--${trend}`}>
          {formatSignedPercent(ratio * 100)}
        </span>
      );
    })();

    return [
      {
        key: 'valuation',
        title: t('assets.modal.insights.groups.valuation', { defaultValue: 'Valuation' }),
        fields: [
          {
            key: 'currentPrice',
            label: t('assets.modal.insights.currentPrice', { defaultValue: 'Current Price' }),
            value: renderInsightsValue(
              assetInsights?.currentPrice != null
                ? formatCurrency(assetInsights.currentPrice, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'graham',
            label: t('assets.modal.insights.graham', { defaultValue: 'Graham Price' }),
            value: renderInsightsValue(
              assetInsights?.graham != null
                ? formatCurrency(assetInsights.graham, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'bazin',
            label: t('assets.modal.insights.bazin', { defaultValue: 'Bazin Price' }),
            value: renderInsightsValue(
              assetInsights?.bazin != null
                ? formatCurrency(assetInsights.bazin, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'fairPrice',
            label: t('assets.modal.insights.fairPrice', { defaultValue: 'Fair Price' }),
            value: renderInsightsValue(
              assetInsights?.fairPrice != null
                ? formatCurrency(assetInsights.fairPrice, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'marginSafety',
            label: t('assets.modal.insights.marginSafety', { defaultValue: 'Margin of Safety' }),
            value: marginOfSafetyValue,
          },
          {
            key: 'pe',
            label: t('assets.modal.insights.pe', { defaultValue: 'P/L' }),
            value: renderInsightsValue(
              assetInsights?.pe != null
                ? assetInsights.pe.toLocaleString(numberLocale, { maximumFractionDigits: 2 })
                : formatDetailValue(null)
            ),
          },
          {
            key: 'pb',
            label: t('assets.modal.insights.pb', { defaultValue: 'P/VP' }),
            value: renderInsightsValue(
              assetInsights?.pb != null
                ? assetInsights.pb.toLocaleString(numberLocale, { maximumFractionDigits: 2 })
                : formatDetailValue(null)
            ),
          },
        ],
      },
      {
        key: 'fundamentals',
        title: t('assets.modal.insights.groups.fundamentals', { defaultValue: 'Fundamentals' }),
        fields: [
          {
            key: 'roe',
            label: t('assets.modal.insights.roe', { defaultValue: 'ROE' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.roe ?? null, true)),
          },
          {
            key: 'roa',
            label: t('assets.modal.insights.roa', { defaultValue: 'ROA' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.roa ?? null, true)),
          },
          {
            key: 'roic',
            label: t('assets.modal.insights.roic', { defaultValue: 'ROIC' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.roic ?? null, true)),
          },
          {
            key: 'lpa',
            label: t('assets.modal.insights.lpa', { defaultValue: 'LPA (EPS)' }),
            value: renderInsightsValue(
              assetInsights?.lpa != null
                ? assetInsights.lpa.toLocaleString(numberLocale, { maximumFractionDigits: 4 })
                : formatDetailValue(null)
            ),
          },
          {
            key: 'vpa',
            label: t('assets.modal.insights.vpa', { defaultValue: 'VPA (Book Value)' }),
            value: renderInsightsValue(
              assetInsights?.vpa != null
                ? assetInsights.vpa.toLocaleString(numberLocale, { maximumFractionDigits: 4 })
                : formatDetailValue(null)
            ),
          },
          {
            key: 'netMargin',
            label: t('assets.modal.insights.netMargin', { defaultValue: 'Net Margin' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.netMargin ?? null, true)),
          },
          {
            key: 'ebitMargin',
            label: t('assets.modal.insights.ebitMargin', { defaultValue: 'EBIT Margin' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.ebitMargin ?? null, true)),
          },
          {
            key: 'payout',
            label: t('assets.modal.insights.payout', { defaultValue: 'Payout' }),
            value: renderInsightsValue(formatRatioAsPercent(assetInsights?.payout ?? null)),
          },
          {
            key: 'evEbitda',
            label: t('assets.modal.insights.evEbitda', { defaultValue: 'EV/EBITDA' }),
            value: renderInsightsValue(
              assetInsights?.evEbitda != null
                ? assetInsights.evEbitda.toLocaleString(numberLocale, { maximumFractionDigits: 2 })
                : formatDetailValue(null)
            ),
          },
          {
            key: 'netDebtEbitda',
            label: t('assets.modal.insights.netDebtEbitda', { defaultValue: 'Net Debt / EBITDA' }),
            value: renderInsightsValue(
              assetInsights?.netDebtEbitda != null
                ? assetInsights.netDebtEbitda.toLocaleString(numberLocale, { maximumFractionDigits: 2 })
                : formatDetailValue(null)
            ),
          },
        ],
      },
      {
        key: 'profile',
        title: t('assets.modal.insights.groups.profileSource', { defaultValue: 'Company Profile & Data Quality' }),
        fields: [
          {
            key: 'sector',
            label: t('assets.modal.insights.sector', { defaultValue: 'Sector' }),
            value: renderInsightsValue(assetInsights?.sector || formatDetailValue(null)),
          },
          {
            key: 'industry',
            label: t('assets.modal.insights.industry', { defaultValue: 'Industry / Segment' }),
            value: renderInsightsValue(assetInsights?.industry || formatDetailValue(null)),
          },
          {
            key: 'marketCap',
            label: t('assets.modal.insights.marketCap', { defaultValue: 'Market Cap' }),
            value: renderInsightsValue(
              assetInsights?.marketCap != null
                ? formatCurrency(assetInsights.marketCap, selectedAsset.currency || 'BRL', numberLocale)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'averageVolume',
            label: t('assets.modal.insights.averageVolume', { defaultValue: 'Avg Volume' }),
            value: renderInsightsValue(
              assetInsights?.averageVolume != null
                ? formatCompactNumber(assetInsights.averageVolume)
                : formatDetailValue(null)
            ),
          },
          {
            key: 'sourceLabel',
            label: t('assets.modal.insights.source', { defaultValue: 'Data Source' }),
            value: renderInsightsValue(assetInsights?.source || formatDetailValue(null)),
          },
          {
            key: 'fetchedAt',
            label: t('assets.modal.insights.fetchedAt', { defaultValue: 'Last Sync' }),
            value: renderInsightsValue(
              assetInsights?.fetchedAt
                ? formatDate(assetInsights.fetchedAt, numberLocale)
                : formatDetailValue(null)
            ),
          },
        ],
      },
    ];
  }, [assetInsights, formatCompactNumber, formatDetailValue, formatPercent, formatSignedPercent, numberLocale, selectedAsset, t]);

  const insightsLinksContent = useMemo<React.ReactNode>(() => {
    if (!assetInsights || assetInsights.status === 'loading') return t('common.loading');
    if (assetInsights.status === 'error') {
      return t('assets.modal.insights.unavailable', { defaultValue: 'Unavailable' });
    }
    if (insightsLinks.length === 0) return formatDetailValue(null);

    return (
      <span className="assets-page__insights-links">
        {insightsLinks.map((link, index) => (
          <span key={link.key}>
            <a
              href={link.href}
              target="_blank"
              rel="noreferrer"
              className="assets-page__provents-source-link"
            >
              {link.label}
            </a>
            {index < insightsLinks.length - 1 ? <span className="assets-page__insights-separator"> • </span> : null}
          </span>
        ))}
      </span>
    );
  }, [assetInsights, formatDetailValue, insightsLinks, t]);

  const financialStatementOptions = useMemo(() => ([
    {
      value: 'income',
      label: t('assets.modal.financials.statement.income', { defaultValue: 'Income Statement' }),
    },
    {
      value: 'balance',
      label: t('assets.modal.financials.statement.balance', { defaultValue: 'Balance Sheet' }),
    },
    {
      value: 'cashflow',
      label: t('assets.modal.financials.statement.cashflow', { defaultValue: 'Cash Flow' }),
    },
  ]), [t]);

  const financialFrequencyOptions = useMemo(() => ([
    {
      value: 'annual',
      label: t('assets.modal.financials.frequency.annual', { defaultValue: 'Annual' }),
    },
    {
      value: 'quarterly',
      label: t('assets.modal.financials.frequency.quarterly', { defaultValue: 'Quarterly' }),
    },
  ]), [t]);

  const selectedFinancialPayload = useMemo<unknown>(() => {
    if (!assetFinancials) return null;
    const key = FINANCIAL_STATEMENT_KEY_MAP[selectedFinancialStatement][selectedFinancialFrequency];
    return assetFinancials[key] ?? null;
  }, [assetFinancials, selectedFinancialFrequency, selectedFinancialStatement]);

  const parsedFinancialStatement = useMemo<ParsedFinancialStatement>(() => (
    parseFinancialStatementPayload(selectedFinancialPayload)
  ), [selectedFinancialPayload]);

  const financialPeriodColumns = useMemo(() => (
    [...parsedFinancialStatement.periods].reverse()
  ), [parsedFinancialStatement.periods]);

  const formatFinancialPeriodLabel = useCallback((period: string): string => {
    const quarterMatch = period.match(/^(\d{4})-Q([1-4])$/);
    if (quarterMatch) {
      return `Q${quarterMatch[2]} ${quarterMatch[1]}`;
    }

    const isoMatch = period.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!isoMatch) return period;

    const date = new Date(Date.UTC(Number(isoMatch[1]), Number(isoMatch[2]) - 1, Number(isoMatch[3])));
    return date.toLocaleDateString(numberLocale, {
      month: 'short',
      year: 'numeric',
      timeZone: 'UTC',
    });
  }, [numberLocale]);

  const formatFinancialValue = useCallback((value: number | null) => {
    if (value === null || !Number.isFinite(value)) return formatDetailValue(null);

    const absolute = Math.abs(value);
    if (absolute >= 1_000_000_000) {
      return value.toLocaleString(numberLocale, {
        notation: 'compact',
        maximumFractionDigits: 2,
      });
    }

    if (absolute >= 1_000_000) {
      return value.toLocaleString(numberLocale, {
        notation: 'compact',
        maximumFractionDigits: 2,
      });
    }

    return value.toLocaleString(numberLocale, {
      maximumFractionDigits: 2,
    });
  }, [formatDetailValue, numberLocale]);

  const financialMetricOptions = useMemo(() => (
    parsedFinancialStatement.rows.map((row) => ({
      value: row.key,
      label: row.label,
    }))
  ), [parsedFinancialStatement.rows]);

  const financialMetricDropdownOptions = useMemo(() => {
    if (financialMetricOptions.length > 0) return financialMetricOptions;
    return [{
      value: '',
      label: t('assets.modal.financials.noMetric', { defaultValue: 'No metrics available' }),
    }];
  }, [financialMetricOptions, t]);

  useEffect(() => {
    if (financialMetricOptions.length === 0) {
      setSelectedFinancialMetric('');
      return;
    }

    if (financialMetricOptions.some((option) => option.value === selectedFinancialMetric)) return;
    setSelectedFinancialMetric(financialMetricOptions[0].value);
  }, [financialMetricOptions, selectedFinancialMetric]);

  const selectedFinancialRow = useMemo(() => (
    parsedFinancialStatement.rows.find((row) => row.key === selectedFinancialMetric)
    || null
  ), [parsedFinancialStatement.rows, selectedFinancialMetric]);

  const selectedFinancialMetricLabel = useMemo(() => {
    if (!selectedFinancialRow) return null;
    return selectedFinancialRow.label;
  }, [selectedFinancialRow]);

  const financialSeries = useMemo<FinancialSeriesPoint[]>(() => {
    if (!selectedFinancialRow) return [];
    return parsedFinancialStatement.periods.map((period) => ({
      period,
      label: formatFinancialPeriodLabel(period),
      value: selectedFinancialRow.valuesByPeriod[period] ?? null,
    }));
  }, [formatFinancialPeriodLabel, parsedFinancialStatement.periods, selectedFinancialRow]);

  const selectedFinancialMetricStats = useMemo(() => {
    if (!selectedFinancialRow) return null;

    const validPeriods = parsedFinancialStatement.periods.filter((period) => (
      selectedFinancialRow.valuesByPeriod[period] !== undefined
      && selectedFinancialRow.valuesByPeriod[period] !== null
    ));
    if (validPeriods.length === 0) return null;

    const latestPeriod = validPeriods[validPeriods.length - 1];
    const latestValue = selectedFinancialRow.valuesByPeriod[latestPeriod] ?? null;
    const previousPeriod = validPeriods.length > 1 ? validPeriods[validPeriods.length - 2] : null;
    const previousValue = previousPeriod ? (selectedFinancialRow.valuesByPeriod[previousPeriod] ?? null) : null;
    const delta = latestValue !== null && previousValue !== null ? latestValue - previousValue : null;
    const deltaPct = delta !== null && previousValue !== null && Math.abs(previousValue) > Number.EPSILON
      ? (delta / previousValue) * 100
      : null;

    return {
      latestPeriod,
      latestValue,
      previousPeriod,
      previousValue,
      delta,
      deltaPct,
    };
  }, [parsedFinancialStatement.periods, selectedFinancialRow]);

  const financialDocuments = useMemo<AssetFinancialDocument[]>(() => (
    parseFinancialDocumentPayload(assetFinancials?.documents)
  ), [assetFinancials?.documents]);

  const fundGeneralInfo = useMemo<AssetFundGeneralInfo | null>(() => (
    parseFundGeneralInfoPayload(assetFinancials?.fund_info)
  ), [assetFinancials?.fund_info]);

  const fundPortfolioRows = useMemo<AssetFundPortfolioRow[]>(() => (
    parseFundPortfolioPayload(assetFinancials?.fund_portfolio)
  ), [assetFinancials?.fund_portfolio]);

  const portfolioCityAllocations = useMemo<PortfolioCityAllocation[]>(() => {
    const rows = fundPortfolioRows.filter((row) => (
      String(row.source || '').toLowerCase().includes('fundsexplorer')
      && Boolean(row.category)
      && Number.isFinite(row.allocationPct)
      && row.allocationPct > 0
    ));
    if (rows.length === 0) return [];

    const byCity = new Map<string, PortfolioCityAllocation>();
    for (const row of rows) {
      const normalizedCity = normalizeCityLabel(row.category);
      if (!normalizedCity) continue;
      const cityKey = cityLabelToKey(normalizedCity);
      if (!cityKey) continue;

      const existing = byCity.get(cityKey);
      if (existing) {
        existing.allocationPct += row.allocationPct;
        if (row.source && !existing.sources.includes(row.source)) {
          existing.sources.push(row.source);
        }
      } else {
        byCity.set(cityKey, {
          cityKey,
          city: normalizedCity,
          allocationPct: row.allocationPct,
          sources: row.source ? [row.source] : [],
        });
      }
    }

    return Array.from(byCity.values())
      .map((row) => ({ ...row, allocationPct: Number(row.allocationPct.toFixed(4)) }))
      .sort((left, right) => right.allocationPct - left.allocationPct);
  }, [fundPortfolioRows]);

  const portfolioCityPoints = useMemo<PortfolioCityPoint[]>(() => (
    portfolioCityAllocations
      .map((row) => {
        const coords = portfolioCityCoordinates[row.cityKey];
        if (!coords || !Number.isFinite(coords.lat) || !Number.isFinite(coords.lon)) return null;
        return {
          ...row,
          lat: coords.lat,
          lon: coords.lon,
          displayName: coords.displayName,
        };
      })
      .filter((row): row is PortfolioCityPoint => Boolean(row))
  ), [portfolioCityAllocations, portfolioCityCoordinates]);

  useEffect(() => {
    if (portfolioCityAllocations.length === 0) {
      setPortfolioCityLoading(false);
      return;
    }

    const missing = portfolioCityAllocations
      .filter((row) => !(row.cityKey in portfolioCityCoordinates))
      .slice(0, CITY_GEOCODE_LIMIT);
    if (missing.length === 0) {
      setPortfolioCityLoading(false);
      return;
    }

    let cancelled = false;
    setPortfolioCityLoading(true);

    const resolveCities = async () => {
      for (const city of missing) {
        if (cancelled) return;
        try {
          const query = encodeURIComponent(`${city.city}, Brasil`);
          const response = await fetch(
            `https://nominatim.openstreetmap.org/search?format=json&limit=1&countrycodes=br&q=${query}`,
            { headers: { Accept: 'application/json' } }
          );
          if (cancelled) return;
          if (!response.ok) {
            setPortfolioCityCoordinates((prev) => ({ ...prev, [city.cityKey]: null }));
            continue;
          }
          const payload = await response.json();
          if (cancelled) return;
          const first = Array.isArray(payload) && payload.length > 0 ? payload[0] : null;
          const lat = first ? Number(first.lat) : NaN;
          const lon = first ? Number(first.lon) : NaN;
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
            setPortfolioCityCoordinates((prev) => ({ ...prev, [city.cityKey]: null }));
            continue;
          }
          setPortfolioCityCoordinates((prev) => ({
            ...prev,
            [city.cityKey]: {
              lat,
              lon,
              displayName: toNonEmptyString(first.display_name),
            },
          }));
        } catch {
          setPortfolioCityCoordinates((prev) => ({ ...prev, [city.cityKey]: null }));
        }

        // Respect free geocoder limits by spacing requests.
        await new Promise((resolve) => setTimeout(resolve, 280));
      }
    };

    resolveCities()
      .catch(() => null)
      .finally(() => {
        if (!cancelled) setPortfolioCityLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [portfolioCityAllocations, portfolioCityCoordinates]);

  useEffect(() => {
    if (activeTab !== 'portfolio') {
      if (portfolioLeafletMapRef.current) {
        portfolioLeafletMapRef.current.remove();
        portfolioLeafletMapRef.current = null;
        portfolioLeafletLayerRef.current = null;
        portfolioLeafletScaleControlRef.current = null;
        portfolioLeafletContainerRef.current = null;
      }
      return;
    }
    if (!portfolioMapContainerRef.current) return;

    let cancelled = false;
    setPortfolioMapError(null);

    ensureLeafletLoaded()
      .then((L) => {
        if (cancelled || !L || !portfolioMapContainerRef.current) return;
        const container = portfolioMapContainerRef.current;

        if (
          portfolioLeafletMapRef.current
          && portfolioLeafletContainerRef.current
          && portfolioLeafletContainerRef.current !== container
        ) {
          portfolioLeafletMapRef.current.remove();
          portfolioLeafletMapRef.current = null;
          portfolioLeafletLayerRef.current = null;
          portfolioLeafletScaleControlRef.current = null;
          portfolioLeafletContainerRef.current = null;
        }

        if (!portfolioLeafletMapRef.current) {
          const map = L.map(container, {
            zoomControl: true,
            scrollWheelZoom: true,
          });
          L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; OpenStreetMap contributors',
          }).addTo(map);
          portfolioLeafletMapRef.current = map;
          portfolioLeafletContainerRef.current = container;
        }

        const map = portfolioLeafletMapRef.current;
        if (!portfolioLeafletScaleControlRef.current) {
          portfolioLeafletScaleControlRef.current = L.control.scale({ imperial: false }).addTo(map);
        }

        if (portfolioLeafletLayerRef.current) {
          map.removeLayer(portfolioLeafletLayerRef.current);
          portfolioLeafletLayerRef.current = null;
        }

        const markers = portfolioCityPoints.map((point) => {
          const marker = L.marker([point.lat, point.lon]);
          marker.bindPopup(`
            <strong>${point.city}</strong><br/>
            ${t('assets.modal.portfolio.map.popup.allocation', { defaultValue: 'Allocation' })}: ${formatPercent(point.allocationPct / 100)}<br/>
            ${t('assets.modal.portfolio.map.popup.source', { defaultValue: 'Source' })}: ${(point.sources.join(', ') || '-')}
          `);
          return marker;
        });

        const layer = L.layerGroup(markers);
        layer.addTo(map);
        portfolioLeafletLayerRef.current = layer;

        if (markers.length > 0) {
          const featureGroup = L.featureGroup(markers);
          map.fitBounds(featureGroup.getBounds(), { padding: [28, 28], maxZoom: 11 });
        } else {
          map.setView(BRAZIL_MAP_DEFAULT_CENTER, BRAZIL_MAP_DEFAULT_ZOOM);
        }

        setTimeout(() => {
          map.invalidateSize();
        }, 0);
      })
      .catch(() => {
        if (cancelled) return;
        setPortfolioMapError(t('assets.modal.portfolio.map.loadError', { defaultValue: 'Unable to load map.' }));
      });

    return () => {
      cancelled = true;
    };
  }, [activeTab, formatPercent, portfolioCityPoints, t]);

  useEffect(() => () => {
    if (portfolioLeafletMapRef.current) {
      portfolioLeafletMapRef.current.remove();
      portfolioLeafletMapRef.current = null;
      portfolioLeafletLayerRef.current = null;
      portfolioLeafletScaleControlRef.current = null;
      portfolioLeafletContainerRef.current = null;
    }
  }, []);

  const shouldRenderFundInfo = useMemo(() => {
    if (!selectedAsset) return false;
    const isFii = String(selectedAsset.assetClass || '').toLowerCase() === 'fii';
    return isFii || fundGeneralInfo !== null;
  }, [fundGeneralInfo, selectedAsset]);

  const shouldRenderFundPortfolio = useMemo(() => {
    if (!selectedAsset) return false;
    const isFii = String(selectedAsset.assetClass || '').toLowerCase() === 'fii';
    return isFii || fundPortfolioRows.length > 0;
  }, [fundPortfolioRows.length, selectedAsset]);

  const isFiiAsset = useMemo(() => {
    if (!selectedAsset) return false;
    return String(selectedAsset.assetClass || '').toLowerCase() === 'fii';
  }, [selectedAsset]);

  const isStockAsset = useMemo(() => {
    if (!selectedAsset) return false;
    return isStockAssetClass(selectedAsset.assetClass);
  }, [selectedAsset]);

  const newsTickerTokens = useMemo(() => (
    buildTickerRelevanceTokens(selectedAsset?.ticker || null)
  ), [selectedAsset?.ticker]);

  const newsAssetNameTokens = useMemo(() => (
    buildAssetNameRelevanceTokens(selectedAsset?.name || null)
  ), [selectedAsset?.name]);

  const scoredAssetNews = useMemo<AssetNewsScoredItem[]>(() => (
    assetNews.map((item) => scoreAssetNewsItem(item, newsTickerTokens, newsAssetNameTokens))
  ), [assetNews, newsAssetNameTokens, newsTickerTokens]);

  const relevantAssetNews = useMemo<AssetNewsScoredItem[]>(() => {
    const strictMatches = scoredAssetNews.filter((item) => item.relevanceScore >= NEWS_RELEVANCE_MIN_SCORE);
    if (strictMatches.length > 0) {
      return [...strictMatches].sort((left, right) => {
        if (right.publishedAtTs !== left.publishedAtTs) return right.publishedAtTs - left.publishedAtTs;
        return right.relevanceScore - left.relevanceScore;
      });
    }

    const relaxedTickerMatches = scoredAssetNews.filter((item) => (
      item.hasTickerMatch
      && item.relevanceScore >= NEWS_RELEVANCE_MIN_SCORE_RELAXED
    ));
    if (relaxedTickerMatches.length > 0) {
      return [...relaxedTickerMatches].sort((left, right) => {
        if (right.publishedAtTs !== left.publishedAtTs) return right.publishedAtTs - left.publishedAtTs;
        return right.relevanceScore - left.relevanceScore;
      });
    }

    return scoredAssetNews
      .filter((item) => item.hasTickerMatch)
      .sort((left, right) => {
        if (right.publishedAtTs !== left.publishedAtTs) return right.publishedAtTs - left.publishedAtTs;
        return right.relevanceScore - left.relevanceScore;
      })
      .slice(0, 8);
  }, [scoredAssetNews]);

  const newsTilesPerRowOptions = useMemo(() => [1, 2, 3, 4], []);

  const newsGridStyle = useMemo<CSSProperties>(() => (
    { '--asset-news-columns': String(newsTilesPerRow) } as CSSProperties
  ), [newsTilesPerRow]);

  const handleNewsTilesPerRowChange = useCallback((value: number) => {
    setNewsTilesPerRow(clampNewsTilesPerRow(value));
  }, []);

  const tabOptions = useMemo(() => {
    const tabs: Array<{ value: AssetDetailsTab; label: string }> = [
      { value: 'overview', label: t('assets.detail.tabs.overview', { defaultValue: 'Overview' }) },
      { value: 'financials', label: t('assets.detail.tabs.financials', { defaultValue: 'Financials' }) },
      { value: 'history', label: t('assets.detail.tabs.history', { defaultValue: 'History' }) },
      { value: 'news', label: t('assets.detail.tabs.news', { defaultValue: 'News' }) },
    ];
    if (shouldRenderFundPortfolio) {
      tabs.splice(1, 0, { value: 'portfolio', label: t('assets.detail.tabs.portfolio', { defaultValue: 'Portfolio' }) });
    }
    if (isFiiAsset) {
      const financialsIndex = tabs.findIndex((tab) => tab.value === 'financials');
      tabs.splice(financialsIndex, 0, { value: 'emissions', label: t('assets.detail.tabs.emissions', { defaultValue: 'Emissões' }) });
    }
    return tabs;
  }, [t, shouldRenderFundPortfolio, isFiiAsset]);

  const fundGeneralInfoFields = useMemo<Array<{ key: string; label: string; value: React.ReactNode }>>(() => {
    const quotaCountValue = fundGeneralInfo?.quotaCount ?? null;
    const quotaCountDisplay = quotaCountValue !== null && Number.isFinite(quotaCountValue)
      ? quotaCountValue.toLocaleString(numberLocale, {
        maximumFractionDigits: Number.isInteger(quotaCountValue) ? 0 : 4,
      })
      : formatDetailValue(null);

    const selectedTicker = normalizeComparableText(selectedAsset?.ticker || null);
    const selectedName = normalizeComparableText(selectedAsset?.name || null);
    const selectedSource = normalizeComparableText(assetInsights?.source || selectedAsset?.source || null);
    const fundTradingCode = normalizeComparableText(fundGeneralInfo?.tradingCode || null);
    const fundTradingName = normalizeComparableText(fundGeneralInfo?.tradingName || null);
    const fundAcronym = normalizeComparableText(fundGeneralInfo?.acronym || null);
    const fundSource = normalizeComparableText(fundGeneralInfo?.source || null);

    const shouldHideTradingCode = Boolean(fundTradingCode) && fundTradingCode === selectedTicker;
    const shouldHideTradingName = Boolean(fundTradingName) && fundTradingName === selectedName;
    const shouldHideAcronym = Boolean(fundAcronym) && selectedTicker.startsWith(fundAcronym);
    const shouldHideSource = Boolean(fundSource) && fundSource === selectedSource;

    return [
      {
        key: 'cnpj',
        label: t('assets.modal.fundInfo.cnpj', { defaultValue: 'CNPJ' }),
        value: formatCnpjValue(fundGeneralInfo?.cnpj || null),
      },
      ...(shouldHideAcronym ? [] : [{
        key: 'acronym',
        label: t('assets.modal.fundInfo.acronym', { defaultValue: 'Acronym' }),
        value: formatDetailValue(fundGeneralInfo?.acronym ?? null),
      }]),
      {
        key: 'legalName',
        label: t('assets.modal.fundInfo.legalName', { defaultValue: 'Legal Name' }),
        value: formatDetailValue(fundGeneralInfo?.legalName ?? null),
      },
      ...(shouldHideTradingName ? [] : [{
        key: 'tradingName',
        label: t('assets.modal.fundInfo.tradingName', { defaultValue: 'Trading Name' }),
        value: formatDetailValue(fundGeneralInfo?.tradingName ?? null),
      }]),
      {
        key: 'classification',
        label: t('assets.modal.fundInfo.classification', { defaultValue: 'Classification' }),
        value: formatDetailValue(fundGeneralInfo?.classification ?? null),
      },
      {
        key: 'segment',
        label: t('assets.modal.fundInfo.segment', { defaultValue: 'Segment' }),
        value: formatDetailValue(fundGeneralInfo?.segment ?? null),
      },
      {
        key: 'administrator',
        label: t('assets.modal.fundInfo.administrator', { defaultValue: 'Administrator' }),
        value: formatDetailValue(fundGeneralInfo?.administrator ?? null),
      },
      {
        key: 'managerName',
        label: t('assets.modal.fundInfo.managerName', { defaultValue: 'Manager' }),
        value: formatDetailValue(fundGeneralInfo?.managerName ?? null),
      },
      {
        key: 'bookkeeper',
        label: t('assets.modal.fundInfo.bookkeeper', { defaultValue: 'Bookkeeper' }),
        value: formatDetailValue(fundGeneralInfo?.bookkeeper ?? null),
      },
      {
        key: 'quotaCount',
        label: t('assets.modal.fundInfo.quotaCount', { defaultValue: 'Quota Count' }),
        value: quotaCountDisplay,
      },
      {
        key: 'quotaDateApproved',
        label: t('assets.modal.fundInfo.quotaDateApproved', { defaultValue: 'Quota Date Approved' }),
        value: fundGeneralInfo?.quotaDateApproved
          ? formatDate(fundGeneralInfo.quotaDateApproved, numberLocale)
          : formatDetailValue(null),
      },
      ...(shouldHideTradingCode ? [] : [{
        key: 'tradingCode',
        label: t('assets.modal.fundInfo.tradingCode', { defaultValue: 'Trading Code' }),
        value: formatDetailValue(fundGeneralInfo?.tradingCode ?? null),
      }]),
      {
        key: 'tradingCodeOthers',
        label: t('assets.modal.fundInfo.tradingCodeOthers', { defaultValue: 'Other Trading Codes' }),
        value: formatDetailValue(fundGeneralInfo?.tradingCodeOthers ?? null),
      },
      {
        key: 'website',
        label: t('assets.modal.fundInfo.website', { defaultValue: 'Website' }),
        value: fundGeneralInfo?.website ? (
          <a href={fundGeneralInfo.website} target="_blank" rel="noreferrer" className="asset-details-page__document-title">
            {fundGeneralInfo.website}
          </a>
        ) : formatDetailValue(null),
      },
      {
        key: 'b3Details',
        label: t('assets.modal.fundInfo.b3Details', { defaultValue: 'B3 Details' }),
        value: fundGeneralInfo?.b3DetailsUrl ? (
          <a href={fundGeneralInfo.b3DetailsUrl} target="_blank" rel="noreferrer" className="asset-details-page__document-title">
            {t('assets.modal.fundInfo.openB3', { defaultValue: 'Open B3 page' })}
          </a>
        ) : formatDetailValue(null),
      },
      {
        key: 'address',
        label: t('assets.modal.fundInfo.address', { defaultValue: 'Address' }),
        value: formatDetailValue(fundGeneralInfo?.address ?? null),
      },
      {
        key: 'phone',
        label: t('assets.modal.fundInfo.phone', { defaultValue: 'Phone' }),
        value: formatDetailValue(fundGeneralInfo?.phone ?? null),
      },
      ...(shouldHideSource ? [] : [{
        key: 'source',
        label: t('assets.modal.fundInfo.source', { defaultValue: 'Source' }),
        value: formatDetailValue(fundGeneralInfo?.source ?? null),
      }]),
    ];
  }, [assetInsights?.source, formatCnpjValue, formatDetailValue, fundGeneralInfo, numberLocale, selectedAsset?.name, selectedAsset?.source, selectedAsset?.ticker, t]);

  const fundSummaryParagraphs = useMemo(() => {
    const explicitParagraphs = stripFundSummaryIntroLines(
      normalizeSummaryTextParagraphs(
        fundGeneralInfo?.descriptionHtml ?? fundGeneralInfo?.description ?? null
      ),
      selectedAsset?.ticker ?? null,
      fundGeneralInfo?.legalName ?? null
    );
    if (explicitParagraphs.length > 0) return explicitParagraphs;

    const fallbackParagraphs = stripFundSummaryIntroLines([
      toNonEmptyString(fundGeneralInfo?.legalName),
      toNonEmptyString(fundGeneralInfo?.classification)
        ? `${t('assets.modal.fundInfo.classification', { defaultValue: 'Classification' })}: ${fundGeneralInfo?.classification}`
        : null,
      toNonEmptyString(fundGeneralInfo?.segment)
        ? `${t('assets.modal.fundInfo.segment', { defaultValue: 'Segment' })}: ${fundGeneralInfo?.segment}`
        : null,
      toNonEmptyString(fundGeneralInfo?.administrator)
        ? `${t('assets.modal.fundInfo.administrator', { defaultValue: 'Administrator' })}: ${fundGeneralInfo?.administrator}`
        : null,
      toNonEmptyString(fundGeneralInfo?.managerName)
        ? `${t('assets.modal.fundInfo.managerName', { defaultValue: 'Manager' })}: ${fundGeneralInfo?.managerName}`
        : null,
    ].filter(Boolean) as string[], selectedAsset?.ticker ?? null, fundGeneralInfo?.legalName ?? null);
    return fallbackParagraphs;
  }, [fundGeneralInfo, selectedAsset?.ticker, t]);

  const fundSummaryText = useMemo(() => (
    fundSummaryParagraphs.join('\n\n').trim() || null
  ), [fundSummaryParagraphs]);

  const fundDividendsResume = useMemo(() => (
    fundGeneralInfo?.dividendsResume || null
  ), [fundGeneralInfo?.dividendsResume]);

  const fundDividendYieldComparator = useMemo(() => (
    fundGeneralInfo?.dividendYieldComparator || null
  ), [fundGeneralInfo?.dividendYieldComparator]);

  const fundDividendYieldComparatorBars = useMemo(() => {
    if (!fundDividendYieldComparator || fundDividendYieldComparator.items.length === 0) return null;

    const parsedItems = fundDividendYieldComparator.items.map((item) => ({
      ...item,
      numericValue: parseLocalizedNumber(item.value),
    }));

    const numericValues = parsedItems
      .map((item) => item.numericValue)
      .filter((value): value is number => value !== null && Number.isFinite(value) && value >= 0);
    const maxNumericValue = numericValues.length > 0 ? Math.max(...numericValues) : null;

    const scoreValues = parsedItems
      .map((item) => item.score)
      .filter((value): value is number => value !== null && Number.isFinite(value) && value >= 0);
    const maxScoreValue = scoreValues.length > 0 ? Math.max(...scoreValues) : null;

    const items = parsedItems.map((item) => {
      const fromNumericValue = (
        maxNumericValue !== null
        && maxNumericValue > 0
        && item.numericValue !== null
        && item.numericValue >= 0
      ) ? (item.numericValue / maxNumericValue) * 100 : null;
      const fromScoreValue = (
        fromNumericValue === null
        && maxScoreValue !== null
        && maxScoreValue > 0
        && item.score !== null
        && item.score >= 0
      ) ? (item.score / maxScoreValue) * 100 : null;
      const widthPercent = Math.max(0, Math.min(100, fromNumericValue ?? fromScoreValue ?? 0));

      return {
        ...item,
        widthPercent,
      };
    });

    const maxLabel = (
      parsedItems.find((item) => item.numericValue !== null && item.numericValue === maxNumericValue)?.value
      ?? (maxNumericValue !== null
        ? `${maxNumericValue.toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`
        : null)
    );

    return {
      items,
      maxLabel,
    };
  }, [fundDividendYieldComparator, numberLocale]);

  const fallbackFinancialDocuments = useMemo(() => {
    if (!selectedAsset) return [];
    const links = buildAssetExternalLinks(selectedAsset.ticker, selectedAsset.assetClass);
    const list = [
      {
        key: 'status-invest',
        url: links.statusInvestUrl,
        title: t('assets.modal.documents.fallback.statusInvest', { defaultValue: 'Status Invest page' }),
      },
      {
        key: 'b3',
        url: links.b3Url,
        title: t('assets.modal.documents.fallback.b3', { defaultValue: 'B3 listed funds page' }),
      },
      {
        key: 'clube-fii',
        url: links.clubeFiiUrl,
        title: t('assets.modal.documents.fallback.clubeFii', { defaultValue: 'Clube FII page' }),
      },
      {
        key: 'fiis',
        url: links.fiisUrl,
        title: t('assets.modal.documents.fallback.fiis', { defaultValue: 'FIIs.com.br page' }),
      },
    ];

    return list.filter((entry): entry is { key: string; url: string; title: string } => Boolean(entry.url));
  }, [selectedAsset, t]);

  const documentCategoryOptions = useMemo(() => {
    const unique = Array.from(new Set(
      financialDocuments
        .map((entry) => toNonEmptyString(entry.category))
        .filter((entry): entry is string => Boolean(entry))
    )).sort((left, right) => left.localeCompare(right, numberLocale));

    return [
      { value: 'all', label: t('assets.modal.documents.filters.all', { defaultValue: 'All' }) },
      ...unique.map((value) => ({ value, label: toDisplayLabel(value) })),
    ];
  }, [financialDocuments, numberLocale, t]);

  const documentTypeOptions = useMemo(() => {
    const unique = Array.from(new Set(
      financialDocuments
        .map((entry) => toNonEmptyString(entry.documentType))
        .filter((entry): entry is string => Boolean(entry))
    )).sort((left, right) => left.localeCompare(right, numberLocale));

    return [
      { value: 'all', label: t('assets.modal.documents.filters.all', { defaultValue: 'All' }) },
      ...unique.map((value) => ({ value, label: toDisplayLabel(value) })),
    ];
  }, [financialDocuments, numberLocale, t]);

  const filteredFinancialDocuments = useMemo(() => (
    financialDocuments.filter((entry) => {
      if (selectedDocumentCategory !== 'all' && entry.category !== selectedDocumentCategory) return false;
      if (selectedDocumentType !== 'all' && entry.documentType !== selectedDocumentType) return false;
      return true;
    })
  ), [financialDocuments, selectedDocumentCategory, selectedDocumentType]);

  useEffect(() => {
    if (documentCategoryOptions.some((entry) => entry.value === selectedDocumentCategory)) return;
    setSelectedDocumentCategory('all');
  }, [documentCategoryOptions, selectedDocumentCategory]);

  useEffect(() => {
    if (documentTypeOptions.some((entry) => entry.value === selectedDocumentType)) return;
    setSelectedDocumentType('all');
  }, [documentTypeOptions, selectedDocumentType]);

  const UPDATES_CATEGORY_MAP: Record<string, string> = useMemo(() => ({
    'Fato Relevante': 'fato_relevante',
    'Fatos Relevantes': 'fato_relevante',
    'Relatórios': 'relatorio',
    'Informes Periódicos': 'informe',
    'Assembleia': 'assembleia',
    'Aviso aos Cotistas': 'outros',
    'Aviso aos Cotistas - Estruturado': 'outros',
    'Comunicado ao Mercado': 'outros',
    'Oferta Pública de Distribuição de Cotas': 'outros',
    'Regulamento': 'outros',
    'Atos de Deliberação do Administrador': 'outros',
  }), []);

  const UPDATES_CATEGORY_LABELS: Record<string, string> = useMemo(() => ({
    relatorio: t('assets.modal.updates.categories.relatorio', { defaultValue: 'Reports' }),
    assembleia: t('assets.modal.updates.categories.assembleia', { defaultValue: 'Assemblies' }),
    fato_relevante: t('assets.modal.updates.categories.fatoRelevante', { defaultValue: 'Material Facts' }),
    informe: t('assets.modal.updates.categories.informe', { defaultValue: 'Periodic Reports' }),
    outros: t('assets.modal.updates.categories.outros', { defaultValue: 'Other' }),
  }), [t]);

  const updatesTimeline = useMemo(() => {
    if (fiiUpdates.length === 0) return [];

    type FiiUpdateItem = typeof fiiUpdates[number];
    const resolveCategory = (item: FiiUpdateItem): string => {
      const cat = item.category || '';
      for (const [prefix, mapped] of Object.entries(UPDATES_CATEGORY_MAP)) {
        if (cat.startsWith(prefix)) return mapped;
      }
      return 'outros';
    };

    const byDate = new Map<string, Array<{ item: FiiUpdateItem; category: string }>>();
    for (const item of fiiUpdates) {
      const rawDate = item.deliveryDate || item.referenceDate || '';
      const date = rawDate.includes('T') ? rawDate.split('T')[0] : rawDate;
      if (!date) continue;
      if (!byDate.has(date)) byDate.set(date, []);
      byDate.get(date)!.push({ item, category: resolveCategory(item) });
    }

    return Array.from(byDate.entries())
      .sort(([a], [b]) => b.localeCompare(a))
      .map(([date, items]) => ({ date, items }));
  }, [fiiUpdates, UPDATES_CATEGORY_MAP]);

  const [selectedUpdateCategories, setSelectedUpdateCategories] = useState<Set<string>>(
    new Set(['relatorio', 'assembleia', 'fato_relevante', 'informe', 'outros'])
  );

  const filteredUpdatesTimeline = useMemo(() => (
    updatesTimeline
      .map(({ date, items }) => ({
        date,
        items: items.filter(({ category }) => selectedUpdateCategories.has(category)),
      }))
      .filter(({ items }) => items.length > 0)
  ), [updatesTimeline, selectedUpdateCategories]);

  const updatesCategoryCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const { items } of updatesTimeline) {
      for (const { category } of items) {
        counts[category] = (counts[category] || 0) + 1;
      }
    }
    return counts;
  }, [updatesTimeline]);

  return (
    <Layout>
      <div className="asset-details-page">
        <div className="asset-details-page__header">
          <div>
            <h1 className="asset-details-page__title">
              {selectedAsset
                ? `${selectedAsset.ticker} • ${selectedAsset.name}`
                : t('assets.detail.title', { defaultValue: 'Asset Details' })}
            </h1>
            <p className="asset-details-page__subtitle">
              {t('assets.detail.subtitle', { defaultValue: 'Complete detail view for the selected asset.' })}
            </p>
          </div>
          <button
            type="button"
            className="asset-details-page__back"
            onClick={() => {
              const query = portfolioId ? `?portfolioId=${encodeURIComponent(portfolioId)}` : '';
              navigate(`/assets${query}`);
            }}
          >
            {t('assets.detail.backToAssets', { defaultValue: 'Back to assets' })}
          </button>
        </div>

        {loading ? (
          <div className="asset-details-page__state">{t('common.loading')}</div>
        ) : null}

        {!loading && !selectedAsset ? (
          <div className="asset-details-page__state">
            {t('assets.detail.notFound', { defaultValue: 'Asset not found for the selected portfolio.' })}
          </div>
        ) : null}

        {!loading && selectedAsset ? (
          <>
            <div className="asset-details-page__tabs" role="tablist" aria-label={t('assets.detail.tabs.ariaLabel', { defaultValue: 'Asset detail sections' })}>
              {tabOptions.map((tab) => {
                const isLoading =
                  (tab.value === 'overview' && assetInsights?.status === 'loading') ||
                  (tab.value === 'portfolio' && assetFinancialsLoading) ||
                  (tab.value === 'emissions' && fiiEmissionsLoading) ||
                  (tab.value === 'financials' && assetFinancialsLoading) ||
                  (tab.value === 'history' && marketSeriesLoading) ||
                  (tab.value === 'news' && assetNewsLoading);
                return (
                  <button
                    key={tab.value}
                    type="button"
                    role="tab"
                    aria-selected={activeTab === tab.value}
                    className={`asset-details-page__tab ${activeTab === tab.value ? 'asset-details-page__tab--active' : ''}`}
                    onClick={() => setActiveTab(tab.value)}
                  >
                    {tab.label}
                    {isLoading ? <span className="asset-details-page__tab-spinner" /> : null}
                  </button>
                );
              })}
            </div>

            {activeTab === 'overview' && (<>
            <div className="asset-details-page__grid">
              <section className="asset-details-page__card">
                <h2>{t('assets.modal.sections.overview')}</h2>
                <dl className="asset-details-page__tiles">
                  {overviewFields.map((field) => (
                    <div
                      key={field.key}
                      className={`asset-details-page__tile ${
                        field.key === 'name' ? 'asset-details-page__tile--wide' : ''
                      }`}
                    >
                      <dt>{field.label}</dt>
                      <dd>{field.value}</dd>
                    </div>
                  ))}
                </dl>
              </section>

              <section className="asset-details-page__card">
                <h2>{t('assets.modal.sections.market')}</h2>
                <dl className="asset-details-page__tiles">
                  {marketFields.map((field) => (
                    <div key={field.key} className="asset-details-page__tile">
                      <dt>{field.label}</dt>
                      <dd>{field.value}</dd>
                    </div>
                  ))}
                </dl>
              </section>

            </div>

            {isStockAsset ? (
            <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--insights">
              <h2>{t('assets.modal.sections.insights', { defaultValue: 'Fundamentals & Fair Value' })}</h2>
              <div className="asset-details-page__insights-grid">
                {insightsGroups.map((group) => (
                  <article key={group.key} className="asset-details-page__insights-group">
                    <h3>{group.title}</h3>
                    <dl className="asset-details-page__tiles asset-details-page__tiles--insights">
                      {group.fields.map((field) => (
                        <div
                          key={`${group.key}-${field.key}`}
                          className={`asset-details-page__tile ${
                            field.key === 'industry' ? 'asset-details-page__tile--wide' : ''
                          }`}
                        >
                          <dt>{field.label}</dt>
                          <dd>{field.value}</dd>
                        </div>
                      ))}
                    </dl>
                  </article>
                ))}
                <article className="asset-details-page__insights-group asset-details-page__insights-group--links">
                  <h3>{t('assets.modal.insights.links.label', { defaultValue: 'External Sources' })}</h3>
                  <div className="asset-details-page__insights-links">{insightsLinksContent}</div>
                </article>
              </div>
            </section>
            ) : null}

            {selectedAssetWeightMetrics ? (
              <section className="asset-details-page__card asset-details-page__card--full">
                <div className="assets-page__weights">
                  <h3>{t('assets.modal.weights.title')}</h3>
                  <div className="assets-page__weights-grid">
                    <article className="assets-page__weight-card">
                      <h4>{t('assets.modal.weights.portfolio')}</h4>
                      <div className="assets-page__weight-chart">
                        <svg viewBox="0 0 120 120" aria-hidden="true">
                          <circle className="assets-page__weight-ring-bg" cx="60" cy="60" r="44" />
                          <circle
                            className="assets-page__weight-ring assets-page__weight-ring--portfolio"
                            cx="60"
                            cy="60"
                            r="44"
                            strokeDasharray={`${2 * Math.PI * 44} ${2 * Math.PI * 44}`}
                            strokeDashoffset={(2 * Math.PI * 44) * (1 - Math.max(0, Math.min(1, selectedAssetWeightMetrics.portfolioWeight)))}
                          />
                        </svg>
                        <div className="assets-page__weight-chart-center">
                          <strong>{formatPercent(selectedAssetWeightMetrics.portfolioWeight)}</strong>
                        </div>
                      </div>
                      <div className="assets-page__weight-meta">
                        <span>{t('assets.modal.weights.assetValue')}: <strong>{formatCurrency(selectedAssetWeightMetrics.selectedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                        <span>{t('assets.modal.weights.portfolioTotal')}: <strong>{formatCurrency(selectedAssetWeightMetrics.portfolioTotal, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                      </div>
                    </article>

                    <article className="assets-page__weight-card">
                      <h4>{t('assets.modal.weights.class', { className: t(`assets.classes.${selectedAsset.assetClass}`, { defaultValue: selectedAsset.assetClass }) })}</h4>
                      <div className="assets-page__weight-chart">
                        <svg viewBox="0 0 120 120" aria-hidden="true">
                          <circle className="assets-page__weight-ring-bg" cx="60" cy="60" r="44" />
                          <circle
                            className="assets-page__weight-ring assets-page__weight-ring--class"
                            cx="60"
                            cy="60"
                            r="44"
                            strokeDasharray={`${2 * Math.PI * 44} ${2 * Math.PI * 44}`}
                            strokeDashoffset={(2 * Math.PI * 44) * (1 - Math.max(0, Math.min(1, selectedAssetWeightMetrics.classWeight)))}
                          />
                        </svg>
                        <div className="assets-page__weight-chart-center">
                          <strong>{formatPercent(selectedAssetWeightMetrics.classWeight)}</strong>
                        </div>
                      </div>
                      <div className="assets-page__weight-meta">
                        <span>{t('assets.modal.weights.assetValue')}: <strong>{formatCurrency(selectedAssetWeightMetrics.selectedCurrentValue, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                        <span>{t('assets.modal.weights.classTotal')}: <strong>{formatCurrency(selectedAssetWeightMetrics.classTotal, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                      </div>
                    </article>
                  </div>
                </div>
              </section>
            ) : null}

            {fundDividendYieldComparator && fundDividendYieldComparatorBars ? (
              <section className="asset-details-page__card asset-details-page__card--full asset-details-page__dy-comparator">
                <h2>{t('assets.modal.sections.dividendYieldComparator', { defaultValue: 'Dividend Yield Comparator' })}</h2>
                {fundDividendYieldComparator.title ? (
                  <h3 className="asset-details-page__dy-comparator-title">{fundDividendYieldComparator.title}</h3>
                ) : null}
                {fundDividendYieldComparator.description ? (
                  <p className="asset-details-page__dy-comparator-description">{fundDividendYieldComparator.description}</p>
                ) : null}

                <div className="asset-details-page__dy-comparator-axis" aria-hidden="true">
                  <span>0%</span>
                  <span>{fundDividendYieldComparatorBars.maxLabel || '100%'}</span>
                </div>
                <div className="asset-details-page__dy-comparator-list">
                  {fundDividendYieldComparatorBars.items.map((item, index) => {
                    const fallbackLabel = item.kind === 'principal'
                      ? (selectedAsset?.ticker || t('assets.modal.dyComparator.labels.principal', { defaultValue: 'Asset' }))
                      : item.kind === 'sector'
                        ? t('assets.modal.dyComparator.labels.sector', { defaultValue: 'Sector' })
                        : item.kind === 'category'
                          ? t('assets.modal.dyComparator.labels.category', { defaultValue: 'Category' })
                          : item.kind === 'market'
                            ? t('assets.modal.dyComparator.labels.market', { defaultValue: 'Market' })
                            : t('assets.modal.dyComparator.labels.reference', { defaultValue: 'Reference' });
                    return (
                      <article
                        key={`dy-comparator-${item.kind}-${index}`}
                        className={`asset-details-page__dy-comparator-item asset-details-page__dy-comparator-item--${item.kind}`}
                      >
                        <div className="asset-details-page__dy-comparator-row">
                          <span className="asset-details-page__dy-comparator-label">{item.label || fallbackLabel}</span>
                          <strong className="asset-details-page__dy-comparator-value">{item.value || formatDetailValue(null)}</strong>
                        </div>
                        {item.detail ? (
                          <div className="asset-details-page__dy-comparator-detail">{item.detail}</div>
                        ) : null}
                        <div className="asset-details-page__dy-comparator-progress">
                          <div
                            className="asset-details-page__dy-comparator-progress-bar"
                            style={{ width: `${item.widthPercent}%` }}
                          />
                        </div>
                      </article>
                    );
                  })}
                </div>
              </section>
            ) : null}

            {(isFiiAsset || shouldRenderFundInfo) ? (
              <section className="asset-details-page__card asset-details-page__card--full">
                <h2 className="asset-details-page__summary-heading">
                  {t('assets.modal.sections.summary', { defaultValue: 'Summary' })}
                  {assetFinancialsLoading ? (
                    <span
                      className="asset-details-page__inline-spinner"
                      role="status"
                      aria-label={t('common.loading')}
                    />
                  ) : null}
                </h2>
                {fundSummaryText ? (
                  <ExpandableText
                    text={fundSummaryText}
                    maxLines={6}
                    expandLabel={t('common.readMore', { defaultValue: 'Read more' })}
                    collapseLabel={t('common.showLess', { defaultValue: 'Show less' })}
                    className="asset-details-page__summary-expandable"
                  />
                ) : (
                  <div className="asset-details-page__summary asset-details-page__summary--text">
                    <p>{formatDetailValue(null)}</p>
                  </div>
                )}
              </section>
            ) : null}

            {fundDividendsResume ? (
              <section className="asset-details-page__card asset-details-page__card--full">
                <h2>{t('assets.modal.sections.dividendsResume', { defaultValue: 'Dividends Snapshot' })}</h2>
                {fundDividendsResume.title ? (
                  <h3 className="asset-details-page__dividends-resume-title">{fundDividendsResume.title}</h3>
                ) : null}

                {fundDividendsResume.paragraphs.map((paragraph, index) => (
                  <p key={`dividends-resume-paragraph-${index}`} className="asset-details-page__dividends-resume-paragraph">
                    {paragraph}
                  </p>
                ))}

                {fundDividendsResume.table ? (
                  <div className="asset-details-page__dividends-resume-table-wrap">
                    <table className="asset-details-page__dividends-resume-table">
                      <thead>
                        <tr>
                          <th>{t('assets.modal.dividendsResume.table.metric', { defaultValue: 'Metric' })}</th>
                          {fundDividendsResume.table.periods.map((period) => (
                            <th key={`dividends-resume-period-${period}`}>{period}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        <tr>
                          <th>{fundDividendsResume.table.returnByUnitLabel || t('assets.modal.dividendsResume.table.returnByUnit', { defaultValue: 'Return per quota' })}</th>
                          {fundDividendsResume.table.returnByUnit.map((value, index) => (
                            <td key={`dividends-resume-return-${index}`}>{value}</td>
                          ))}
                        </tr>
                        <tr>
                          <th>{fundDividendsResume.table.relativeToQuoteLabel || t('assets.modal.dividendsResume.table.relativeToQuote', { defaultValue: 'Relative to current quote' })}</th>
                          {fundDividendsResume.table.relativeToQuote.map((value, index) => (
                            <td key={`dividends-resume-relative-${index}`}>{value}</td>
                          ))}
                        </tr>
                      </tbody>
                    </table>
                  </div>
                ) : null}
              </section>
            ) : null}

            {shouldRenderFundInfo ? (
              <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--two-cols">
                <h2>{t('assets.modal.sections.fundInfo', { defaultValue: 'General Info' })}</h2>
                <dl>
                  {fundGeneralInfoFields.map((field) => (
                    <div key={field.key}>
                      <dt>{field.label}</dt>
                      <dd>{field.value}</dd>
                    </div>
                  ))}
                </dl>
              </section>
            ) : null}

            </>)}

            {activeTab === 'financials' && (<>
            {String(selectedAsset?.assetClass || '').toLowerCase() !== 'fii' ? (
            <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--financials">
              <div className="asset-details-page__financials-header">
                <h2>{t('assets.modal.financials.title', { defaultValue: 'Financial Statements' })}</h2>
                <div className="asset-details-page__financials-controls">
                  <SharedDropdown
                    value={selectedFinancialStatement}
                    options={financialStatementOptions}
                    onChange={(value) => {
                      if (value === 'income' || value === 'balance' || value === 'cashflow') {
                        setSelectedFinancialStatement(value);
                      }
                    }}
                    ariaLabel={t('assets.modal.financials.statement.label', { defaultValue: 'Statement' })}
                    className="asset-details-page__dropdown"
                    size="sm"
                  />
                  <SharedDropdown
                    value={selectedFinancialFrequency}
                    options={financialFrequencyOptions}
                    onChange={(value) => {
                      if (value === 'annual' || value === 'quarterly') {
                        setSelectedFinancialFrequency(value);
                      }
                    }}
                    ariaLabel={t('assets.modal.financials.frequency.label', { defaultValue: 'Frequency' })}
                    className="asset-details-page__dropdown"
                    size="sm"
                  />
                  <SharedDropdown
                    value={selectedFinancialMetric}
                    options={financialMetricDropdownOptions}
                    onChange={setSelectedFinancialMetric}
                    ariaLabel={t('assets.modal.financials.metric', { defaultValue: 'Metric' })}
                    className="asset-details-page__dropdown asset-details-page__dropdown--metric"
                    size="sm"
                    disabled={financialMetricOptions.length === 0}
                  />
                </div>
              </div>

              {assetFinancialsLoading ? (
                <p className="asset-details-page__financials-state">{t('common.loading')}</p>
              ) : null}

              {!assetFinancialsLoading && assetFinancialsError ? (
                <div className="asset-details-page__financials-state asset-details-page__financials-state--error">
                  <p>{t('assets.modal.financials.loadError', { defaultValue: 'Failed to load financial statements.' })}</p>
                  <code>{assetFinancialsError}</code>
                </div>
              ) : null}

              {!assetFinancialsLoading && !assetFinancialsError && parsedFinancialStatement.rows.length === 0 ? (
                <p className="asset-details-page__financials-state">
                  {t('assets.modal.financials.empty', { defaultValue: 'No financial statements available for this asset.' })}
                </p>
              ) : null}

              {!assetFinancialsLoading && !assetFinancialsError && parsedFinancialStatement.rows.length > 0 ? (
                <div className="asset-details-page__financials-content">
                  <div className="asset-details-page__financials-chart">
                    <div className="asset-details-page__financials-chart-header">
                      <h3>
                        {t('assets.modal.financials.evolutionTitle', {
                          metric: selectedFinancialMetricLabel || t('assets.modal.financials.metric', { defaultValue: 'Metric' }),
                          defaultValue: '{{metric}} evolution',
                        })}
                      </h3>
                      {selectedFinancialMetricStats ? (
                        <div className="asset-details-page__financials-stats">
                          <span>
                            {t('assets.modal.financials.latest', { defaultValue: 'Latest' })}:
                            {' '}
                            <strong>
                              {formatFinancialPeriodLabel(selectedFinancialMetricStats.latestPeriod)}
                              {' '}
                              •
                              {' '}
                              {formatFinancialValue(selectedFinancialMetricStats.latestValue)}
                            </strong>
                          </span>
                          {selectedFinancialMetricStats.previousPeriod ? (
                            <span>
                              {t('assets.modal.financials.previous', { defaultValue: 'Previous' })}:
                              {' '}
                              <strong>
                                {formatFinancialPeriodLabel(selectedFinancialMetricStats.previousPeriod)}
                                {' '}
                                •
                                {' '}
                                {formatFinancialValue(selectedFinancialMetricStats.previousValue)}
                              </strong>
                            </span>
                          ) : null}
                          {selectedFinancialMetricStats.delta !== null ? (
                            <span>
                              {t('assets.modal.financials.delta', { defaultValue: 'Delta' })}:
                              {' '}
                              <strong className={selectedFinancialMetricStats.delta >= 0 ? 'assets-page__delta assets-page__delta--positive' : 'assets-page__delta assets-page__delta--negative'}>
                                {formatFinancialValue(selectedFinancialMetricStats.delta)}
                                {selectedFinancialMetricStats.deltaPct !== null
                                  ? ` (${formatSignedPercent(selectedFinancialMetricStats.deltaPct)})`
                                  : ''}
                              </strong>
                            </span>
                          ) : null}
                        </div>
                      ) : null}
                    </div>
                    <div className="asset-details-page__financials-chart-canvas">
                      <ResponsiveContainer width="100%" height={260}>
                        <LineChart data={financialSeries} margin={{ top: 8, right: 20, bottom: 8, left: 8 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.2)" />
                          <XAxis
                            dataKey="label"
                            tick={{ fill: 'var(--text-secondary)', fontSize: 12 }}
                            interval="preserveStartEnd"
                          />
                          <YAxis
                            tick={{ fill: 'var(--text-secondary)', fontSize: 12 }}
                            tickFormatter={(value: number) => formatFinancialValue(value)}
                            width={96}
                          />
                          <Tooltip
                            formatter={(value: unknown) => {
                              const numeric = toNumericValue(value);
                              return [
                                formatFinancialValue(numeric),
                                selectedFinancialMetricLabel || t('assets.modal.financials.metric', { defaultValue: 'Metric' }),
                              ];
                            }}
                            labelFormatter={(label) => `${t('assets.modal.financials.period', { defaultValue: 'Period' })}: ${label}`}
                          />
                          <Line
                            dataKey="value"
                            type="monotone"
                            stroke="var(--accent-primary, #22d3ee)"
                            strokeWidth={2}
                            dot={{ r: 3, strokeWidth: 1 }}
                            activeDot={{ r: 5 }}
                            connectNulls
                          />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </div>

                  <div className="asset-details-page__financials-table-wrap">
                    <table className="asset-details-page__financials-table">
                      <thead>
                        <tr>
                          <th>{t('assets.modal.financials.metric', { defaultValue: 'Metric' })}</th>
                          {financialPeriodColumns.map((period) => (
                            <th key={period}>{formatFinancialPeriodLabel(period)}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {parsedFinancialStatement.rows.map((row) => (
                          <tr key={row.key}>
                            <th scope="row">{row.label}</th>
                            {financialPeriodColumns.map((period) => (
                              <td key={`${row.key}-${period}`}>
                                {formatFinancialValue(row.valuesByPeriod[period] ?? null)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : null}
            </section>
            ) : null}

            {String(selectedAsset?.assetClass || '').toLowerCase() !== 'fii' ? (
            <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--documents">
              <div className="asset-details-page__documents-header">
                <h2>{t('assets.modal.documents.title', { defaultValue: 'Documents & Filings' })}</h2>
                <div className="asset-details-page__documents-header-right">
                  {!assetFinancialsLoading && financialDocuments.length > 0 ? (
                    <span className="asset-details-page__documents-count">
                      {t('assets.modal.documents.count', {
                        defaultValue: '{{count}} documents',
                        count: filteredFinancialDocuments.length,
                      })}
                    </span>
                  ) : null}
                  {!assetFinancialsLoading && financialDocuments.length > 0 ? (
                    <div className="asset-details-page__documents-controls">
                      <SharedDropdown
                        value={selectedDocumentCategory}
                        options={documentCategoryOptions}
                        onChange={setSelectedDocumentCategory}
                        ariaLabel={t('assets.modal.documents.filters.category', { defaultValue: 'Category filter' })}
                        className="asset-details-page__dropdown"
                        size="sm"
                      />
                      <SharedDropdown
                        value={selectedDocumentType}
                        options={documentTypeOptions}
                        onChange={setSelectedDocumentType}
                        ariaLabel={t('assets.modal.documents.filters.type', { defaultValue: 'Type filter' })}
                        className="asset-details-page__dropdown"
                        size="sm"
                      />
                    </div>
                  ) : null}
                </div>
              </div>

              {assetFinancialsLoading ? (
                <p className="asset-details-page__financials-state">{t('common.loading')}</p>
              ) : null}

              {!assetFinancialsLoading && financialDocuments.length === 0 ? (
                <div className="asset-details-page__documents-empty">
                  <p className="asset-details-page__financials-state">
                    {t('assets.modal.documents.empty', {
                      defaultValue: 'No filing documents were returned for this asset yet.',
                    })}
                  </p>
                  {fallbackFinancialDocuments.length > 0 ? (
                    <div className="asset-details-page__documents-fallback">
                      {fallbackFinancialDocuments.map((entry) => (
                        <a
                          key={entry.key}
                          href={entry.url}
                          target="_blank"
                          rel="noreferrer"
                          className="asset-details-page__document-fallback-link"
                        >
                          {entry.title}
                        </a>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : null}

              {!assetFinancialsLoading && financialDocuments.length > 0 ? (
                <>
                  {filteredFinancialDocuments.length === 0 ? (
                    <p className="asset-details-page__financials-state">
                      {t('assets.modal.documents.emptyFiltered', {
                        defaultValue: 'No documents match the selected filters.',
                      })}
                    </p>
                  ) : (
                    <div className="asset-details-page__documents-table-wrap">
                      <table className="asset-details-page__documents-table">
                        <thead>
                          <tr>
                            <th>{t('assets.modal.documents.columns.date', { defaultValue: 'Date' })}</th>
                            <th>{t('assets.modal.documents.columns.category', { defaultValue: 'Category' })}</th>
                            <th>{t('assets.modal.documents.columns.type', { defaultValue: 'Type' })}</th>
                            <th>{t('assets.modal.documents.columns.title', { defaultValue: 'Document' })}</th>
                            <th>{t('assets.modal.documents.columns.source', { defaultValue: 'Source' })}</th>
                            <th>{t('assets.modal.documents.columns.status', { defaultValue: 'Status' })}</th>
                            <th>{t('assets.modal.documents.columns.action', { defaultValue: 'Action' })}</th>
                          </tr>
                        </thead>
                        <tbody>
                          {filteredFinancialDocuments.map((document) => {
                            const sourceLabel = (document.source || '')
                              .replace(/[_-]+/g, ' ')
                              .trim()
                              .toUpperCase();
                            const primaryDate = document.deliveryDate || document.referenceDate || null;
                            return (
                              <tr key={`${document.url}|${document.id || ''}`}>
                                <td>{primaryDate ? formatDate(primaryDate, numberLocale) : formatDetailValue(null)}</td>
                                <td>{document.category ? toDisplayLabel(document.category) : formatDetailValue(null)}</td>
                                <td>{document.documentType ? toDisplayLabel(document.documentType) : formatDetailValue(null)}</td>
                                <td>
                                  <a
                                    href={document.url}
                                    target="_blank"
                                    rel="noreferrer"
                                    className="asset-details-page__document-title"
                                  >
                                    {document.title || t('assets.modal.documents.untitled', { defaultValue: 'Financial filing' })}
                                  </a>
                                </td>
                                <td>{sourceLabel || formatDetailValue(null)}</td>
                                <td>{document.status || formatDetailValue(null)}</td>
                                <td>
                                  <a
                                    href={document.url}
                                    target="_blank"
                                    rel="noreferrer"
                                    className="asset-details-page__document-action"
                                  >
                                    {t('assets.modal.documents.open', { defaultValue: 'Open' })}
                                  </a>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  )}
                </>
              ) : null}
            </section>
            ) : null}

            {updatesTimeline.length > 0 || fiiUpdatesLoading ? (
              <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--updates">
                <div className="asset-details-page__updates-header">
                  <h2>{t('assets.modal.updates.title', { defaultValue: 'Updates' })}</h2>
                  {!fiiUpdatesLoading && updatesTimeline.length > 0 ? (
                    <span className="asset-details-page__updates-count">
                      {t('assets.modal.updates.count', {
                        defaultValue: '{{count}} updates',
                        count: filteredUpdatesTimeline.reduce((total, group) => total + group.items.length, 0),
                      })}
                    </span>
                  ) : null}
                </div>

                {fiiUpdatesLoading ? (
                  <p className="asset-details-page__financials-state">{t('common.loading')}</p>
                ) : null}

                {!fiiUpdatesLoading && updatesTimeline.length > 0 ? (
                  <>
                    <div className="asset-details-page__updates-filters">
                      {(['relatorio', 'assembleia', 'fato_relevante', 'informe', 'outros'] as const).map((cat) => (
                        <label key={cat} className="asset-details-page__updates-filter">
                          <input
                            type="checkbox"
                            checked={selectedUpdateCategories.has(cat)}
                            onChange={() => {
                              setSelectedUpdateCategories((prev) => {
                                const next = new Set(prev);
                                if (next.has(cat)) next.delete(cat);
                                else next.add(cat);
                                return next;
                              });
                            }}
                          />
                          <span className="asset-details-page__updates-filter-label">
                            {UPDATES_CATEGORY_LABELS[cat] || cat}
                          </span>
                          <span className="asset-details-page__updates-filter-count">
                            ({updatesCategoryCounts[cat] || 0})
                          </span>
                        </label>
                      ))}
                    </div>

                    {filteredUpdatesTimeline.length === 0 ? (
                      <p className="asset-details-page__financials-state">
                        {t('assets.modal.updates.empty', { defaultValue: 'No updates match the selected filters.' })}
                      </p>
                    ) : (
                      <div className="asset-details-page__updates-timeline">
                        {filteredUpdatesTimeline.map(({ date, items }) => (
                          <div key={date} className="asset-details-page__updates-group">
                            <div className="asset-details-page__updates-date">
                              {formatDate(date, numberLocale)}
                            </div>
                            <div className="asset-details-page__updates-items">
                              {items.map(({ item, category }) => (
                                <a
                                  key={`${item.url}|${item.id}`}
                                  href={item.url}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="asset-details-page__updates-item"
                                >
                                  <span className={`asset-details-page__updates-category asset-details-page__updates-category--${category}`}>
                                    {UPDATES_CATEGORY_LABELS[category] || category}
                                  </span>
                                  <span className="asset-details-page__updates-item-title">
                                    {item.title}
                                  </span>
                                </a>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </>
                ) : null}
              </section>
            ) : null}
            </>)}

            {activeTab === 'history' && (<>
            <section className="asset-details-page__card asset-details-page__card--full">
              <div className="assets-page__history">
                <h3>{t('assets.modal.priceHistory.title')}</h3>
                <div className="asset-details-page__chart-controls">
                  <label htmlFor="asset-period-select">
                    {t('assets.modal.priceHistory.periodLabel', { defaultValue: 'Period' })}
                  </label>
                  <select
                    id="asset-period-select"
                    className="asset-details-page__chart-select"
                    value={chartPeriod}
                    onChange={(event) => {
                      setChartPeriod(event.target.value as ChartPeriodPreset);
                      setHoveredMarketPointIndex(null);
                    }}
                  >
                    <option value="MAX">{t('assets.modal.priceHistory.period.max', { defaultValue: 'MAX' })}</option>
                    <option value="5A">{t('assets.modal.priceHistory.period.5a', { defaultValue: '5Y' })}</option>
                    <option value="2A">{t('assets.modal.priceHistory.period.2a', { defaultValue: '2Y' })}</option>
                    <option value="1A">{t('assets.modal.priceHistory.period.1a', { defaultValue: '1Y' })}</option>
                    <option value="6M">{t('assets.modal.priceHistory.period.6m', { defaultValue: '6M' })}</option>
                    <option value="3M">{t('assets.modal.priceHistory.period.3m', { defaultValue: '3M' })}</option>
                    <option value="1M">{t('assets.modal.priceHistory.period.1m', { defaultValue: '1M' })}</option>
                    <option value="CUSTOM">{t('assets.modal.priceHistory.period.custom', { defaultValue: 'Custom' })}</option>
                  </select>
                  {chartPeriod === 'CUSTOM' ? (
                    <div className="asset-details-page__chart-range">
                      <label htmlFor="asset-range-start">
                        {t('assets.modal.priceHistory.startDate', { defaultValue: 'From' })}
                      </label>
                      <input
                        id="asset-range-start"
                        type="date"
                        value={customRangeStart}
                        min={minSelectableStartDate || undefined}
                        max={todayIso}
                        onChange={(event) => {
                          const normalized = toIsoDate(event.target.value) || '';
                          if (!normalized) {
                            setCustomRangeStart('');
                            return;
                          }
                          let nextStart = normalized;
                          if (minSelectableStartDate && nextStart < minSelectableStartDate) {
                            nextStart = minSelectableStartDate;
                          }
                          if (nextStart > todayIso) nextStart = todayIso;
                          setCustomRangeStart(nextStart);
                          if (customRangeEnd && customRangeEnd < nextStart) {
                            setCustomRangeEnd(nextStart);
                          }
                        }}
                      />

                      <label htmlFor="asset-range-end">
                        {t('assets.modal.priceHistory.endDate', { defaultValue: 'To' })}
                      </label>
                      <input
                        id="asset-range-end"
                        type="date"
                        value={customRangeEnd}
                        min={customRangeStart || minSelectableStartDate || undefined}
                        max={todayIso}
                        onChange={(event) => {
                          const normalized = toIsoDate(event.target.value) || '';
                          if (!normalized) {
                            setCustomRangeEnd('');
                            return;
                          }
                          let nextEnd = normalized;
                          if (nextEnd > todayIso) nextEnd = todayIso;
                          if (customRangeStart && nextEnd < customRangeStart) {
                            nextEnd = customRangeStart;
                          }
                          if (minSelectableStartDate && nextEnd < minSelectableStartDate) {
                            nextEnd = minSelectableStartDate;
                          }
                          setCustomRangeEnd(nextEnd);
                        }}
                      />
                    </div>
                  ) : null}
                </div>

                {marketSeriesLoading ? (
                  <p className="assets-page__history-empty">{t('assets.modal.priceHistory.loading')}</p>
                ) : null}
                {!marketSeriesLoading && !marketPriceChart ? (
                  <p className="assets-page__history-empty">{t('assets.modal.priceHistory.empty')}</p>
                ) : null}
                {!marketSeriesLoading && marketPriceChart ? (
                  <div className="assets-page__market-chart">
                    <svg
                      viewBox={`0 0 ${HISTORY_CHART_WIDTH} ${HISTORY_CHART_HEIGHT}`}
                      role="img"
                      aria-label={t('assets.modal.priceHistory.chart')}
                      onMouseMove={(event) => {
                        const bounds = event.currentTarget.getBoundingClientRect();
                        const relativeX = ((event.clientX - bounds.left) / bounds.width) * HISTORY_CHART_WIDTH;

                        let nearestIndex = 0;
                        let nearestDistance = Number.POSITIVE_INFINITY;
                        marketPriceChart.points.forEach((point, index) => {
                          const distance = Math.abs(point.x - relativeX);
                          if (distance < nearestDistance) {
                            nearestDistance = distance;
                            nearestIndex = index;
                          }
                        });

                        setHoveredMarketPointIndex(nearestIndex);
                      }}
                      onMouseLeave={() => setHoveredMarketPointIndex(null)}
                    >
                      <defs>
                        <linearGradient id="asset-market-history-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                          <stop offset="0%" stopColor="rgba(34, 211, 238, 0.34)" />
                          <stop offset="100%" stopColor="rgba(34, 211, 238, 0.02)" />
                        </linearGradient>
                      </defs>
                      <line
                        className="assets-page__history-grid"
                        x1={marketPriceChart.padding.left}
                        x2={HISTORY_CHART_WIDTH - marketPriceChart.padding.right}
                        y1={marketPriceChart.padding.top}
                        y2={marketPriceChart.padding.top}
                      />
                      <line
                        className="assets-page__history-grid"
                        x1={marketPriceChart.padding.left}
                        x2={HISTORY_CHART_WIDTH - marketPriceChart.padding.right}
                        y1={marketPriceChart.padding.top + ((marketPriceChart.yBase - marketPriceChart.padding.top) / 2)}
                        y2={marketPriceChart.padding.top + ((marketPriceChart.yBase - marketPriceChart.padding.top) / 2)}
                      />
                      <line
                        className="assets-page__history-grid"
                        x1={marketPriceChart.padding.left}
                        x2={HISTORY_CHART_WIDTH - marketPriceChart.padding.right}
                        y1={marketPriceChart.yBase}
                        y2={marketPriceChart.yBase}
                      />
                      <path className="assets-page__market-area" d={marketPriceChart.areaPath} fill="url(#asset-market-history-gradient)" />
                      <path className="assets-page__market-line" d={marketPriceChart.polyline} />
                      {hoveredMarketPoint ? (
                        <circle
                          className="assets-page__market-hover-point"
                          cx={hoveredMarketPoint.x}
                          cy={hoveredMarketPoint.y}
                          r={5}
                        />
                      ) : null}
                    </svg>
                    {hoveredMarketPoint && hoveredMarketTooltipStyle ? (
                      <div className="assets-page__history-tooltip" style={hoveredMarketTooltipStyle}>
                        <div className="assets-page__history-tooltip-header">
                          <strong>{formatDate(hoveredMarketPoint.displayDate, numberLocale)}</strong>
                        </div>
                        <div className="assets-page__history-tooltip-grid">
                          <span>{t('assets.modal.priceHistory.close')}</span>
                          <strong>{formatCurrency(hoveredMarketPoint.close, selectedAsset.currency || 'BRL', numberLocale)}</strong>
                          <span>{t('assets.modal.priceHistory.change')}</span>
                          <strong>
                            {hoveredMarketPoint.change === null
                              ? '-'
                              : formatSignedCurrency(hoveredMarketPoint.change, selectedAsset.currency || 'BRL')}
                          </strong>
                          <span>{t('assets.modal.priceHistory.changePct')}</span>
                          <strong>
                            {hoveredMarketPoint.changePct === null
                              ? '-'
                              : formatSignedPercent(hoveredMarketPoint.changePct)}
                          </strong>
                        </div>
                      </div>
                    ) : null}
                    <div className="assets-page__history-scale">
                      <span>{formatDate(marketPriceChart.firstDate, numberLocale)}</span>
                      <span>{formatCurrency(marketPriceChart.minClose, selectedAsset.currency || 'BRL', numberLocale)}</span>
                      <span>{formatCurrency(marketPriceChart.maxClose, selectedAsset.currency || 'BRL', numberLocale)}</span>
                      <span>{formatDate(marketPriceChart.lastDate, numberLocale)}</span>
                    </div>
                    <div className="assets-page__history-meta">
                      <span>{t('assets.modal.priceHistory.lastClose')}: <strong>{formatCurrency(marketPriceChart.lastClose, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                    </div>
                  </div>
                ) : null}

                <h3>{t('assets.modal.history.title')}</h3>
                {assetTradeHistoryRows.length === 0 ? (
                  <p className="assets-page__history-empty">{t('assets.modal.history.empty')}</p>
                ) : (
                  <>
                    <div className="assets-page__history-stats">
                      <span>{t('assets.modal.history.totalTrades')}: <strong>{assetTradeHistoryStats.trades}</strong></span>
                      <span>{t('assets.modal.history.buys')}: <strong>{assetTradeHistoryStats.buys}</strong></span>
                      <span>{t('assets.modal.history.sells')}: <strong>{assetTradeHistoryStats.sells}</strong></span>
                      <span>
                        {t('assets.modal.history.avgBuyPrice')}: <strong>
                          {assetTradeHistoryStats.avgBuyPrice !== null
                            ? formatCurrency(assetTradeHistoryStats.avgBuyPrice, selectedAsset.currency || 'BRL', numberLocale)
                            : '-'}
                        </strong>
                      </span>
                      <span>
                        {t('assets.modal.history.avgSellPrice')}: <strong>
                          {assetTradeHistoryStats.avgSellPrice !== null
                            ? formatCurrency(assetTradeHistoryStats.avgSellPrice, selectedAsset.currency || 'BRL', numberLocale)
                            : '-'}
                        </strong>
                      </span>
                    </div>

                    <div className="assets-page__history-chart">
                      <svg
                        viewBox={`0 0 ${HISTORY_CHART_WIDTH} ${HISTORY_CHART_HEIGHT}`}
                        role="img"
                        aria-label={t('assets.modal.history.chart')}
                        onClick={() => setSelectedTradePoint(null)}
                      >
                        <defs>
                          <linearGradient id="asset-trade-history-gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                            <stop offset="0%" stopColor="rgba(99, 102, 241, 0.42)" />
                            <stop offset="100%" stopColor="rgba(99, 102, 241, 0.02)" />
                          </linearGradient>
                        </defs>
                        {assetTradeHistoryChart ? (
                          <>
                            <line
                              className="assets-page__history-grid"
                              x1={assetTradeHistoryChart.padding.left}
                              x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                              y1={assetTradeHistoryChart.padding.top}
                              y2={assetTradeHistoryChart.padding.top}
                            />
                            <line
                              className="assets-page__history-grid"
                              x1={assetTradeHistoryChart.padding.left}
                              x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                              y1={assetTradeHistoryChart.padding.top + ((assetTradeHistoryChart.yBase - assetTradeHistoryChart.padding.top) / 2)}
                              y2={assetTradeHistoryChart.padding.top + ((assetTradeHistoryChart.yBase - assetTradeHistoryChart.padding.top) / 2)}
                            />
                            <line
                              className="assets-page__history-grid"
                              x1={assetTradeHistoryChart.padding.left}
                              x2={HISTORY_CHART_WIDTH - assetTradeHistoryChart.padding.right}
                              y1={assetTradeHistoryChart.yBase}
                              y2={assetTradeHistoryChart.yBase}
                            />
                            <path className="assets-page__history-area" d={assetTradeHistoryChart.areaPath} fill="url(#asset-trade-history-gradient)" />
                            <path className="assets-page__history-line" d={assetTradeHistoryChart.polyline} />
                            {assetTradeHistoryChart.points.map((point) => (
                              <circle
                                key={`${point.transId}-${point.date}-${point.price}-${point.index}`}
                                className={`assets-page__history-point assets-page__history-point--${point.type}`}
                                cx={point.x}
                                cy={point.y}
                                r={selectedTradePoint?.transId === point.transId && selectedTradePoint?.index === point.index ? 6 : 4}
                                role="button"
                                tabIndex={0}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  setSelectedTradePoint(point);
                                }}
                                onKeyDown={(event) => {
                                  if (event.key === 'Enter' || event.key === ' ') {
                                    event.preventDefault();
                                    setSelectedTradePoint(point);
                                  }
                                }}
                              >
                                <title>
                                  {`${formatDate(point.date, numberLocale)} | ${t(`transactions.types.${point.type}`, { defaultValue: point.type })} | ${formatCurrency(point.price, point.currency, numberLocale)}`}
                                </title>
                              </circle>
                            ))}
                          </>
                        ) : null}
                      </svg>
                      {selectedTradePoint && selectedTradeTooltipStyle ? (
                        <div className="assets-page__history-tooltip" style={selectedTradeTooltipStyle}>
                          <div className="assets-page__history-tooltip-header">
                            <span className={`assets-page__history-type assets-page__history-type--${selectedTradePoint.type}`}>
                              {t(`transactions.types.${selectedTradePoint.type}`, { defaultValue: selectedTradePoint.type })}
                            </span>
                            <strong>{formatDate(selectedTradePoint.date, numberLocale)}</strong>
                          </div>
                          <div className="assets-page__history-tooltip-grid">
                            <span>{t('assets.modal.history.quantity')}</span>
                            <strong>{formatAssetQuantity(selectedTradePoint.quantity)}</strong>
                            <span>{t('assets.modal.history.price')}</span>
                            <strong>{formatCurrency(selectedTradePoint.price, selectedTradePoint.currency, numberLocale)}</strong>
                            <span>{t('assets.modal.history.amount')}</span>
                            <strong>{formatCurrency(selectedTradePoint.amount, selectedTradePoint.currency, numberLocale)}</strong>
                            <span>{t('assets.modal.history.source')}</span>
                            <strong>{selectedTradePoint.source || '-'}</strong>
                          </div>
                        </div>
                      ) : null}
                      {assetTradeHistoryChart ? (
                        <div className="assets-page__history-scale">
                          <span>{formatDate(assetTradeHistoryChart.firstDate, numberLocale)}</span>
                          <span>{formatCurrency(assetTradeHistoryChart.minPrice, selectedAsset.currency || 'BRL', numberLocale)}</span>
                          <span>{formatCurrency(assetTradeHistoryChart.maxPrice, selectedAsset.currency || 'BRL', numberLocale)}</span>
                          <span>{formatDate(assetTradeHistoryChart.lastDate, numberLocale)}</span>
                        </div>
                      ) : null}
                      {assetTradeHistoryChart ? (
                        <div className="assets-page__history-meta">
                          <span>{t('assets.modal.history.lastPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.lastPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                          <span>{t('assets.modal.history.minPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.minPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                          <span>{t('assets.modal.history.maxPrice')}: <strong>{formatCurrency(assetTradeHistoryChart.maxPrice, selectedAsset.currency || 'BRL', numberLocale)}</strong></span>
                        </div>
                      ) : null}
                    </div>

                    <div className="assets-page__history-table-wrap">
                      <table className="assets-page__history-table">
                        <thead>
                          <tr>
                            <th>{t('assets.modal.history.date')}</th>
                            <th>{t('assets.modal.history.type')}</th>
                            <th>{t('assets.modal.history.quantity')}</th>
                            <th>{t('assets.modal.history.price')}</th>
                            <th>{t('assets.modal.history.amount')}</th>
                            <th>{t('assets.modal.history.source')}</th>
                          </tr>
                        </thead>
                        <tbody>
                          {[...assetTradeHistoryRows].reverse().map((row) => (
                            <tr key={`${row.transId}-${row.date}-${row.type}`}>
                              <td>{formatDate(row.date, numberLocale)}</td>
                              <td>
                                <span className={`assets-page__history-type assets-page__history-type--${row.type}`}>
                                  {t(`transactions.types.${row.type}`, { defaultValue: row.type })}
                                </span>
                              </td>
                              <td>{formatAssetQuantity(row.quantity)}</td>
                              <td>{formatCurrency(row.price, row.currency, numberLocale)}</td>
                              <td>{formatCurrency(row.amount, row.currency, numberLocale)}</td>
                              <td>{row.source || '-'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                )}
              </div>
            </section>

            <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--splits">
              <div className="asset-details-page__splits-header">
                <h2>{t('assets.modal.splits.title', { defaultValue: 'Desdobramento / Grupamento' })}</h2>
                {splitEventsCoverage ? (
                  <span className="asset-details-page__splits-meta">
                    {t('assets.modal.splits.period', {
                      start: formatDate(splitEventsCoverage.start, numberLocale),
                      end: formatDate(splitEventsCoverage.end, numberLocale),
                      defaultValue: '{{start}} - {{end}}',
                    })}
                  </span>
                ) : null}
              </div>

              {assetSplitEvents.length === 0 ? (
                <p className="asset-details-page__financials-state">
                  {t('assets.modal.splits.empty', {
                    defaultValue: 'No desdobramento/grupamento events found for this asset.',
                  })}
                </p>
              ) : (
                <div className="asset-details-page__splits-table-wrap">
                  <table className="asset-details-page__splits-table">
                    <thead>
                      <tr>
                        <th>{t('assets.modal.splits.date', { defaultValue: 'Date' })}</th>
                        <th>{t('assets.modal.splits.type', { defaultValue: 'Type' })}</th>
                        <th>{t('assets.modal.splits.ratio', { defaultValue: 'Ratio' })}</th>
                        <th>{t('assets.modal.splits.factor', { defaultValue: 'Factor' })}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {assetSplitEvents.map((event) => (
                        <tr key={`${event.date}-${event.factor}-${event.eventType}`}>
                          <td>{formatDate(event.date, numberLocale)}</td>
                          <td>
                            <span className={`asset-details-page__split-type asset-details-page__split-type--${event.eventType}`}>
                              {t(`assets.modal.splits.types.${event.eventType}`, { defaultValue: event.eventType })}
                            </span>
                          </td>
                          <td>{formatSplitRatio(event.factor)}</td>
                          <td>{formatSplitFactor(event.factor)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              <p className="asset-details-page__splits-source">
                {t('assets.modal.splits.sourceHint', {
                  defaultValue: 'Derived from stock split factors in historical price data.',
                })}
              </p>
            </section>
            </>)}

            {activeTab === 'news' && (<>
            <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--news">
              <div className="asset-details-page__news-header">
                <h2>{t('assets.modal.news.title', { defaultValue: 'Relevant News' })}</h2>
                <div className="asset-details-page__news-header-right">
                  <div
                    className="asset-details-page__news-layout-control"
                    role="group"
                    aria-label={t('assets.modal.news.tilesPerLineAria', { defaultValue: 'Select news tiles per line' })}
                  >
                    <span className="asset-details-page__news-layout-label">
                      {t('assets.modal.news.tilesPerLine', { defaultValue: 'Tiles per line' })}
                    </span>
                    <div className="asset-details-page__news-layout-options">
                      {newsTilesPerRowOptions.map((option) => (
                        <button
                          key={`news-tiles-${option}`}
                          type="button"
                          className={`asset-details-page__news-layout-option ${
                            newsTilesPerRow === option ? 'asset-details-page__news-layout-option--active' : ''
                          }`}
                          onClick={() => handleNewsTilesPerRowChange(option)}
                          aria-pressed={newsTilesPerRow === option}
                        >
                          {option}
                        </button>
                      ))}
                    </div>
                  </div>
                  {!assetNewsLoading ? (
                    <span className="asset-details-page__news-count">
                      {t('assets.modal.news.count', {
                        count: relevantAssetNews.length,
                        defaultValue: '{{count}} relevant articles',
                      })}
                    </span>
                  ) : null}
                </div>
              </div>

              <p className="asset-details-page__news-subtitle">
                {t('assets.modal.news.subtitle', {
                  ticker: selectedAsset.ticker,
                  defaultValue: 'Recent, high-signal headlines related to {{ticker}}.',
                })}
              </p>

              {assetNewsLoading ? (
                <p className="asset-details-page__financials-state">{t('common.loading')}</p>
              ) : null}

              {!assetNewsLoading && assetNewsError ? (
                <div className="asset-details-page__financials-state asset-details-page__financials-state--error">
                  <p>{t('assets.modal.news.loadError', { defaultValue: 'Failed to load news for this asset.' })}</p>
                  <code>{assetNewsError}</code>
                </div>
              ) : null}

              {!assetNewsLoading && !assetNewsError && relevantAssetNews.length === 0 ? (
                <p className="asset-details-page__financials-state">
                  {t('assets.modal.news.empty', { defaultValue: 'No relevant news found for this asset right now.' })}
                </p>
              ) : null}

              {!assetNewsLoading && !assetNewsError && relevantAssetNews.length > 0 ? (
                <div
                  key={`news-layout-${newsLayoutTransitionKey}`}
                  className="asset-details-page__news-list"
                  style={newsGridStyle}
                >
                  {relevantAssetNews.map((item, index) => {
                    const newsCardStyle: CSSProperties = {
                      '--asset-news-stagger': `${Math.min(index, 9) * 38}ms`,
                    } as CSSProperties;
                    if (item.imageUrl) {
                      newsCardStyle.backgroundImage = [
                        'linear-gradient(180deg, rgba(2, 6, 23, 0.18) 0%, rgba(2, 6, 23, 0.72) 54%, rgba(2, 6, 23, 0.92) 100%)',
                        `url("${item.imageUrl.replace(/"/g, '\\"')}")`,
                      ].join(', ');
                      newsCardStyle.backgroundSize = 'cover';
                      newsCardStyle.backgroundPosition = 'center';
                    }

                    return (
                    <article
                      key={item.id}
                      className={`asset-details-page__news-item ${item.imageUrl ? 'asset-details-page__news-item--with-image' : ''}`}
                      style={newsCardStyle}
                    >
                      <div className="asset-details-page__news-item-head">
                        <a
                          href={item.link}
                          target="_blank"
                          rel="noreferrer"
                          className="asset-details-page__news-title-link"
                        >
                          {item.title}
                        </a>
                        <span className="asset-details-page__news-score">
                          {t('assets.modal.news.relevance', { defaultValue: 'Relevance' })}: {item.relevanceScore}
                        </span>
                      </div>

                      {item.description ? (
                        <p className="asset-details-page__news-description">{item.description}</p>
                      ) : (
                        <p className="asset-details-page__news-description asset-details-page__news-description--empty">
                          {formatDetailValue(null)}
                        </p>
                      )}

                      <div className="asset-details-page__news-meta">
                        <span>
                          {t('assets.modal.news.publishedAt', { defaultValue: 'Published' })}: <strong>{formatNewsPublishedAt(item.publishedAt)}</strong>
                        </span>
                        <span>
                          {t('assets.modal.news.source', { defaultValue: 'Source' })}: <strong>{item.sourceLabel}</strong>
                        </span>
                      </div>
                    </article>
                    );
                  })}
                </div>
              ) : null}
            </section>
            </>)}

            {activeTab === 'emissions' && (<>
            <section className="asset-details-page__card asset-details-page__card--full">
              {fiiEmissionsLoading ? (
                <p className="asset-details-page__financials-state">{t('common.loading')}</p>
              ) : (
                <DataTable<FiiEmissionRow>
                  rows={fiiEmissions}
                  rowKey={(row) => `${row.ticker}-${row.emissionNumber}`}
                  columns={[
                    {
                      key: 'emission',
                      label: t('assets.detail.emissions.columns.emission', { defaultValue: 'Emissão' }),
                      sortable: true,
                      sortValue: (row) => row.emissionNumber,
                      render: (row) => `${row.emissionNumber}ª emissão`,
                    },
                    {
                      key: 'stage',
                      label: t('assets.detail.emissions.columns.status', { defaultValue: 'Status' }),
                      sortable: true,
                      sortValue: (row) => row.stage,
                      render: (row) => (
                        <span className={`asset-details-page__emission-stage asset-details-page__emission-stage--${row.stage}`}>
                          {row.stage}
                        </span>
                      ),
                    },
                    {
                      key: 'price',
                      label: t('assets.detail.emissions.columns.price', { defaultValue: 'Preço (R$)' }),
                      sortable: true,
                      sortValue: (row) => row.price ?? 0,
                      render: (row) => row.price !== null
                        ? row.price.toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
                        : '-',
                    },
                    {
                      key: 'discount',
                      label: t('assets.detail.emissions.columns.discount', { defaultValue: 'Desconto' }),
                      sortable: true,
                      sortValue: (row) => row.discount ?? 0,
                      render: (row) => {
                        if (row.discount === null) return '-';
                        const cls = row.discount < 0
                          ? 'asset-details-page__emission-discount--negative'
                          : 'asset-details-page__emission-discount--positive';
                        return (
                          <span className={`asset-details-page__emission-discount ${cls}`}>
                            {row.discount > 0 ? '+' : ''}{row.discount.toLocaleString(numberLocale, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%
                          </span>
                        );
                      },
                    },
                    {
                      key: 'baseDate',
                      label: t('assets.detail.emissions.columns.baseDate', { defaultValue: 'Data-base' }),
                      sortable: true,
                      sortValue: (row) => row.baseDate,
                      render: (row) => row.baseDate || '-',
                    },
                    {
                      key: 'factor',
                      label: t('assets.detail.emissions.columns.factor', { defaultValue: 'Fator' }),
                      sortable: false,
                      render: (row) => row.proportionFactor || '-',
                    },
                    {
                      key: 'preference',
                      label: t('assets.detail.emissions.columns.preference', { defaultValue: 'Preferência' }),
                      sortable: false,
                      render: (row) => (
                        <div className="asset-details-page__emission-period">
                          <span>{row.preferenceStart && row.preferenceStart !== 'A definir' ? `${row.preferenceStart} – ${row.preferenceEnd}` : row.preferenceStart || '-'}</span>
                          {row.preferenceStatus ? (
                            <span className={`asset-details-page__emission-period-status asset-details-page__emission-period-status--${row.preferenceStatus === 'Em andamento' ? 'active' : row.preferenceStatus === 'Data atingida' ? 'done' : 'pending'}`}>
                              {row.preferenceStatus}
                            </span>
                          ) : null}
                        </div>
                      ),
                    },
                    {
                      key: 'sobras',
                      label: t('assets.detail.emissions.columns.sobras', { defaultValue: 'Sobras' }),
                      sortable: false,
                      render: (row) => (
                        <div className="asset-details-page__emission-period">
                          <span>{row.sobrasStart && row.sobrasStart !== 'A definir' ? `${row.sobrasStart} – ${row.sobrasEnd}` : row.sobrasStart || '-'}</span>
                          {row.sobrasStatus ? (
                            <span className={`asset-details-page__emission-period-status asset-details-page__emission-period-status--${row.sobrasStatus === 'Em andamento' ? 'active' : row.sobrasStatus === 'Data atingida' ? 'done' : 'pending'}`}>
                              {row.sobrasStatus}
                            </span>
                          ) : null}
                        </div>
                      ),
                    },
                  ]}
                  searchLabel={t('assets.detail.emissions.search.label', { defaultValue: 'Search' })}
                  searchPlaceholder={t('assets.detail.emissions.search.placeholder', { defaultValue: 'Search emissions...' })}
                  searchTerm={emissionsSearchTerm}
                  onSearchTermChange={setEmissionsSearchTerm}
                  matchesSearch={(row, term) => {
                    const lower = term.toLowerCase();
                    return (
                      row.ticker.toLowerCase().includes(lower) ||
                      String(row.emissionNumber).includes(lower) ||
                      row.stage.toLowerCase().includes(lower)
                    );
                  }}
                  itemsPerPage={emissionsItemsPerPage}
                  onItemsPerPageChange={setEmissionsItemsPerPage}
                  pageSizeOptions={[5, 10, 25]}
                  emptyLabel={t('assets.detail.emissions.empty', { defaultValue: 'No emissions data available for this asset.' })}
                  defaultSort={{ key: 'emission', direction: 'desc' }}
                  labels={{
                    itemsPerPage: t('assets.pagination.itemsPerPage'),
                    prev: t('assets.pagination.prev'),
                    next: t('assets.pagination.next'),
                    page: (page, total) => t('assets.pagination.page', { page, total }),
                    showing: (start, end, total) => t('assets.pagination.showing', { start, end, total }),
                  }}
                />
              )}
            </section>
            </>)}

            {activeTab === 'portfolio' && (<>
            <section className="asset-details-page__card asset-details-page__card--full">
              <DataTable<AssetFundPortfolioRow>
                rows={fundPortfolioRows}
                rowKey={(row) => `${row.label}-${row.allocationPct}-${row.category || ''}`}
                columns={[
                  {
                    key: 'label',
                    label: t('assets.modal.portfolio.columns.label', { defaultValue: 'Item' }),
                    sortable: true,
                    sortValue: (row) => row.label,
                    render: (row) => row.label,
                  },
                  {
                    key: 'allocation',
                    label: t('assets.modal.portfolio.columns.allocation', { defaultValue: 'Allocation' }),
                    sortable: true,
                    sortValue: (row) => row.allocationPct,
                    render: (row) => (
                      <div className="asset-details-page__portfolio-allocation">
                        <span>{formatPercent(row.allocationPct / 100)}</span>
                        <div className="asset-details-page__portfolio-allocation-bar" aria-hidden="true">
                          <span
                            className="asset-details-page__portfolio-allocation-fill"
                            style={{ width: `${Math.max(0, Math.min(100, row.allocationPct))}%` }}
                          />
                        </div>
                      </div>
                    ),
                  },
                  {
                    key: 'category',
                    label: t('assets.modal.portfolio.columns.category', { defaultValue: 'Category' }),
                    sortable: true,
                    sortValue: (row) => row.category || '',
                    render: (row) => row.category || '-',
                  },
                ]}
                searchLabel={t('assets.modal.portfolio.search.label', { defaultValue: 'Search' })}
                searchPlaceholder={t('assets.modal.portfolio.search.placeholder', { defaultValue: 'Search properties...' })}
                searchTerm={portfolioSearchTerm}
                onSearchTermChange={setPortfolioSearchTerm}
                matchesSearch={(row, term) => {
                  const lower = term.toLowerCase();
                  return (row.label?.toLowerCase().includes(lower) || row.category?.toLowerCase().includes(lower)) ?? false;
                }}
                itemsPerPage={portfolioItemsPerPage}
                onItemsPerPageChange={setPortfolioItemsPerPage}
                pageSizeOptions={[5, 10, 25, 50]}
                emptyLabel={t('assets.modal.portfolio.empty', { defaultValue: 'No portfolio composition data available for this asset.' })}
                defaultSort={{ key: 'allocation', direction: 'desc' }}
                labels={{
                  itemsPerPage: t('assets.pagination.itemsPerPage'),
                  prev: t('assets.pagination.prev'),
                  next: t('assets.pagination.next'),
                  page: (page, total) => t('assets.pagination.page', { page, total }),
                  showing: (start, end, total) => t('assets.pagination.showing', { start, end, total }),
                }}
              />
            </section>

            <section className="asset-details-page__card asset-details-page__card--full asset-details-page__card--portfolio-map">
              <div className="asset-details-page__portfolio-map-header">
                <h2>{t('assets.modal.portfolio.map.title', { defaultValue: 'Portfolio Cities Map' })}</h2>
                <span className="asset-details-page__portfolio-map-count">
                  {t('assets.modal.portfolio.map.count', {
                    defaultValue: '{{count}} cities',
                    count: portfolioCityAllocations.length,
                  })}
                </span>
              </div>

              <p className="asset-details-page__portfolio-map-subtitle">
                {t('assets.modal.portfolio.map.subtitle', {
                  defaultValue: 'Map based on properties and city data from portfolio composition sources.',
                })}
              </p>

              {portfolioMapError ? (
                <p className="asset-details-page__financials-state asset-details-page__financials-state--error">
                  {portfolioMapError}
                </p>
              ) : null}

              {portfolioCityLoading ? (
                <p className="asset-details-page__financials-state">
                  {t('assets.modal.portfolio.map.loading', { defaultValue: 'Loading map points...' })}
                </p>
              ) : null}

              {!portfolioCityLoading && portfolioCityAllocations.length === 0 ? (
                <p className="asset-details-page__financials-state">
                  {t('assets.modal.portfolio.map.empty', { defaultValue: 'No city-level portfolio locations available for this asset.' })}
                </p>
              ) : null}

              {portfolioCityAllocations.length > 0 ? (
                <div
                  ref={portfolioMapContainerRef}
                  className="asset-details-page__portfolio-map"
                  role="region"
                  aria-label={t('assets.modal.portfolio.map.ariaLabel', { defaultValue: 'Portfolio cities map' })}
                />
              ) : null}
            </section>
            </>)}
          </>
        ) : null}
      </div>
    </Layout>
  );
};

export default AssetDetailsPage;
