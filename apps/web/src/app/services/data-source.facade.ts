import { Injectable, inject } from '@angular/core';
import { BehaviorSubject, Subscription, interval } from 'rxjs';
import { tap } from 'rxjs/operators';
import { MarketService } from './market.service';
import { OhlcService } from './ohlc.service';
import { TicksSocketService } from './ticks-socket.service';
import { MarketDataService } from './market-data.service';
import { Ohlc } from '../models/ohlc.model';

@Injectable({ providedIn: 'root' })
export class DataSourceFacade {
  private market = inject(MarketService);
  private ohlc = inject(OhlcService);
  private socket = inject(TicksSocketService);
  private md = inject(MarketDataService);

  readonly mode$ = new BehaviorSubject<'LIVE' | 'HISTORIC'>('HISTORIC');
  readonly ceOhlc$ = new BehaviorSubject<Ohlc[]>([]);
  readonly peOhlc$ = new BehaviorSubject<Ohlc[]>([]);
  readonly lastTickAge$ = new BehaviorSubject<number>(0);
  readonly futLtp$ = new BehaviorSubject<number | null>(null);

  private lastTickTs = 0;
  private pollSub?: Subscription;

  constructor() {
    interval(1000).subscribe(() => {
      if (this.lastTickTs > 0) {
        this.lastTickAge$.next(Math.floor((Date.now() - this.lastTickTs) / 1000));
      }
    });
    this.socket.isConnected$.subscribe(c => {
      if (!c) {
        this.mode$.next('HISTORIC');
        this.startPolling();
      }
    });
  }

  init() {
    this.market.getStatus().subscribe(st => {
      this.lastTickTs = st.lastTickTs;
      if (st.online && st.wsConnected) {
        this.mode$.next('LIVE');
        this.loadAndConnect();
      } else {
        this.mode$.next('HISTORIC');
        this.startPolling();
      }
    });
  }

  private loadAndConnect() {
    this.ohlc.getCEPE('CE').subscribe(ce => this.ceOhlc$.next(ce));
    this.ohlc.getCEPE('PE').subscribe(pe => this.peOhlc$.next(pe));
    this.pollSub?.unsubscribe();
    const ceKeys = Array.from(new Set(this.ceOhlc$.getValue().map(o => o.instrumentKey)));
    const peKeys = Array.from(new Set(this.peOhlc$.getValue().map(o => o.instrumentKey)));
    this.md.getSelection().subscribe(sel => {
      const fut = sel?.mainInstrument || '';
      this.socket.connect(fut, ceKeys, peKeys);
      this.socket.fut$.subscribe(t => { this.lastTickTs = new Date(t.ts).getTime(); this.futLtp$.next(t.ltp); });
      this.socket.ce$.subscribe(t => this.mergeTick(this.ceOhlc$, t));
      this.socket.pe$.subscribe(t => this.mergeTick(this.peOhlc$, t));
    });
  }

  private startPolling() {
    this.pollSub?.unsubscribe();
    this.pollSub = interval(5000).pipe(
      tap(() => {
        this.ohlc.getCEPE('CE').subscribe(ce => this.ceOhlc$.next(ce));
        this.ohlc.getCEPE('PE').subscribe(pe => this.peOhlc$.next(pe));
      })
    ).subscribe();
  }

  private mergeTick(stream: BehaviorSubject<Ohlc[]>, tick: any) {
    const arr = [...stream.getValue()];
    const sec = Math.floor(new Date(tick.ts).getTime() / 1000) * 1000;
    let last = arr[arr.length - 1];
    if (!last || last.t < sec) {
      arr.push({ instrumentKey: tick.instrumentKey, t: sec, o: tick.ltp, h: tick.ltp, l: tick.ltp, c: tick.ltp, v: tick.volume || 0 });
      if (arr.length > 900) arr.shift();
    } else {
      last.h = Math.max(last.h, tick.ltp);
      last.l = Math.min(last.l, tick.ltp);
      last.c = tick.ltp;
      last.v += tick.volume || 0;
    }
    stream.next(arr);
  }
}
