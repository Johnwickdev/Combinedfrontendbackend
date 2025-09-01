import { Injectable } from '@angular/core';
import { BehaviorSubject, Subject } from 'rxjs';
import { environment } from '../../environments/environment';

interface TickMsg { instrumentKey: string; ltp: number; ts: string; volume?: number; }

@Injectable({ providedIn: 'root' })
export class TicksSocketService {
  private es: EventSource | null = null;
  private futKey: string | null = null;
  private ceKeys = new Set<string>();
  private peKeys = new Set<string>();

  readonly isConnected$ = new BehaviorSubject<boolean>(false);
  private futSub = new Subject<TickMsg>();
  private ceSub = new Subject<TickMsg>();
  private peSub = new Subject<TickMsg>();
  readonly fut$ = this.futSub.asObservable();
  readonly ce$ = this.ceSub.asObservable();
  readonly pe$ = this.peSub.asObservable();

  connect(futKey: string, ceKeys: string[], peKeys: string[]) {
    this.disconnect();
    this.futKey = futKey;
    this.ceKeys = new Set(ceKeys);
    this.peKeys = new Set(peKeys);
    const all = [futKey, ...ceKeys, ...peKeys].filter(Boolean);
    if (!all.length) return;
    const params = all.map(k => `instrumentKey=${encodeURIComponent(k)}`).join('&');
    const url = `${environment.apiBase}/md/stream?${params}`;
    this.es = new EventSource(url);
    this.es.onopen = () => this.isConnected$.next(true);
    this.es.onerror = () => this.isConnected$.next(false);
    this.es.onmessage = ev => {
      try {
        const t = JSON.parse(ev.data) as TickMsg;
        if (t.instrumentKey === this.futKey) this.futSub.next(t);
        else if (this.ceKeys.has(t.instrumentKey)) this.ceSub.next(t);
        else if (this.peKeys.has(t.instrumentKey)) this.peSub.next(t);
      } catch {}
    };
  }

  disconnect() {
    this.es?.close();
    this.es = null;
    this.isConnected$.next(false);
  }
}
