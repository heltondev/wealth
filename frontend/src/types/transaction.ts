export type TransactionType = 'buy' | 'sell' | 'dividend' | 'jcp' | 'tax' | 'transfer';
export type TransactionStatus = 'confirmed' | 'pending_mapping';

export interface Transaction {
  transId: string;
  portfolioId: string;
  assetId: string;
  type: TransactionType;
  date: string;
  quantity: number;
  price: number;
  currency: string;
  amount: number;
  status: TransactionStatus;
  sourceDocId: string | null;
  createdAt: string;
}
