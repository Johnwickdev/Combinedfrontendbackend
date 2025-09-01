export interface MarketStatus {
  online: boolean;
  wsConnected: boolean;
  lastTickTs: number;
  reason: string;
}
