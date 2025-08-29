import { Injectable, NgZone, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { BehaviorSubject, Observable, Subject, catchError, of, map } from 'rxjs';
import { environment } from '../../environments/environment';
import { TradeRow } from '../models/trade-row';

export interface SectorTradeRow {
  ts: string;
  optionType: 'CE' | 'PE';
  strike: number;
  ltp: number;
  changePct?: number | null;
  qty?: number | null;
  oi?: number | null;
  iv?: number | null;
}

export interface Candle {
  time: string; // ISO timestamp of minute start
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface Tick {
  instrumentKey: string;
  ltp: number;
  ts: string; // ISO timestamp
  volume: number;
}

@Injectable({ providedIn: 'root' })
export class MarketDataService {
  private http = inject(HttpClient);
  private zone = inject(NgZone);
  private apiBase = environment.apiBase;

  private sse: EventSource | null = null;
  private tickSub = new Subject<Tick>();
  private tradeSub = new Subject<TradeRow>();
  private connectionStatus = new BehaviorSubject<boolean>(false);
  connection$ = this.connectionStatus.asObservable();
  ticks$ = this.tickSub.asObservable();
  trades$ = this.tradeSub.asObservable();
  private delays = [1000, 2000, 5000, 10000];
  private delayIndex = 0;

  /** fetch current LTP for the given instrument */
  getLtp(key: string) {
    return this.http
      .get<{
        instrumentKey: string;
        ltp: number;
        ts: string;
        source: 'live' | 'influx';
      }>(`${this.apiBase}/md/ltp?instrumentKey=${encodeURIComponent(key)}`, {
        observe: 'response',
      })
      .pipe(map(res => (res.status === 204 ? null : res.body || null)));
  }

  /** public observable to listen for tick updates */
  listenTicks() {
    return this.ticks$;
  }

  /** public observable to listen for trade updates */
  listenTrades() {
    return this.trades$;
  }

  /** fetch selection of main instrument and options */
  getSelection(): Observable<{ mainInstrument: string; options: string[] } | null> {
    return this.http
      .get<{ mainInstrument: string; options: string[] }>(`${this.apiBase}/md/selection`)
      .pipe(catchError(() => of(null)));
  }

  /** load historical candles */
  getCandles(key: string): Observable<Candle[]> {
    return this.http.get<Candle[]>(`${this.apiBase}/md/candles?instrumentKey=${encodeURIComponent(key)}&tf=1m&lookback=120`);
  }

  getSectorTrades(side: 'both' | 'CE' | 'PE' = 'both', limit = 50): Observable<{ rows: SectorTradeRow[]; source?: 'live' | 'influx' }> {
    const params = new HttpParams().set('limit', limit).set('side', side);
    return this.http
      .get<SectorTradeRow[]>(`${this.apiBase}/md/sector-trades`, { params, observe: 'response' })
      .pipe(
        map(res => {
          const header = res.headers.get('X-Source');
          const rows = (res.body || []).sort((a, b) => new Date(b.ts).getTime() - new Date(a.ts).getTime());
          let source: 'live' | 'influx' | undefined;
          if (header === 'live' || header === 'influx') {
            source = header;
          } else {
            const istNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
            const mins = istNow.getHours() * 60 + istNow.getMinutes();
            const within = mins >= 9 * 60 + 15 && mins <= 15 * 60 + 30;
            if (within) {
              const now = Date.now();
              if (rows.some(r => now - new Date(r.ts).getTime() <= 60000)) {
                source = 'live';
              }
            }
            if (!source) source = 'influx';
          }
          return { rows, source };
        })
      );
  }

  /** connect to the streaming endpoint for the selected instruments */
  connect(keys: string[]) {
    this.disconnect();
    if (!keys.length) return;
    const params = keys.map(k => `instrumentKey=${encodeURIComponent(k)}`).join('&');
    const url = `${this.apiBase}/md/stream?${params}`;
    const open = () => {
      this.sse = new EventSource(url);
      this.connectionStatus.next(true);
      this.sse.onmessage = ev => {
        this.zone.run(() => {
          try {
            const t = JSON.parse(ev.data) as Tick;
            this.tickSub.next(t);
          } catch (e) {}
        });
      };
      this.sse.addEventListener('trade', ev => {
        this.zone.run(() => {
          try {
            const t = JSON.parse((ev as MessageEvent).data) as TradeRow;
            this.tradeSub.next(t);
          } catch (e) {}
        });
      });
      this.sse.onerror = () => {
        this.connectionStatus.next(false);
        this.sse?.close();
        const delay = this.delays[this.delayIndex];
        setTimeout(open, delay);
        if (this.delayIndex < this.delays.length - 1) this.delayIndex++;
      };
    };
    this.delayIndex = 0;
    open();
  }

  disconnect() {
    if (this.sse) {
      this.sse.close();
      this.sse = null;
    }
    this.connectionStatus.next(false);
    this.delayIndex = 0;
  }
}
