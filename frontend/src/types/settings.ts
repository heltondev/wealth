export interface UserSettings {
  displayName?: string;
  email?: string;
  preferredCurrency?: string;
  locale?: string;
  updatedAt?: string;
}

export interface Alias {
  normalizedName: string;
  ticker: string;
  source: string;
  createdAt: string;
}
