export type AssetClass = 'stock' | 'fii' | 'bond' | 'crypto' | 'rsu';
export type Country = 'BR' | 'US' | 'CA';

export interface Asset {
  assetId: string;
  portfolioId: string;
  ticker: string;
  name: string;
  assetClass: AssetClass;
  country: Country;
  currency: string;
  status: string;
  createdAt: string;
}
