export type OptionType = 'CE' | 'PE';

export interface TradeRow {
  ts: string;
  instrumentKey: string;
  optionType: OptionType;
  strike: number;
  ltp: number;
  changePct: number;
  qty: number;
  oi: number;
  txId: string;
}
