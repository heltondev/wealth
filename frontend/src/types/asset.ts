export type AssetClass = string;
export type Country = string;

export interface Asset {
  assetId: string;
  portfolioId: string;
  ticker: string;
  name: string;
  assetClass: AssetClass;
  country: Country;
  currency: string;
  status: string;
  quantity?: number;
  source?: string | null;
  createdAt: string;
}
