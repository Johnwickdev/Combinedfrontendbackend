export type OptionSide = 'CE' | 'PE' | 'both';
export type OptionType = 'CE' | 'PE';

export interface TradeRow {
  ts: string;
  instrumentKey: string;
  optionType: OptionType;
  strike: number;
  ltp: number;
  changePct: number | null;
  qty: number | null;
  oi: number | null;
  txId: string;
}
