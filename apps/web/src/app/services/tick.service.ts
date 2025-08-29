import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable, timer } from 'rxjs';
import { map, shareReplay, switchMap } from 'rxjs/operators';
import { environment } from '../../environments/environment';

/**
 * Service to provide the latest NIFTY FUT last traded price (LTP).
 * Polls the backend `/md/ltp` endpoint every second and shares the
 * most recent value with all subscribers.  Each poll expects a response
 * shaped as `{ ltp: number }`.
 */
@Injectable({ providedIn: 'root' })
export class TickService {
  private http = inject(HttpClient);
  private apiBase = environment.apiBase;

  /** Stream of LTP values refreshed on every poll */
  private ltp$ = timer(0, 1000).pipe(
    switchMap(() => this.http.get<{ ltp: number }>(`${this.apiBase}/md/ltp`)),
    map(res => res.ltp),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  /** Public observable for subscribers */
  getLtp(): Observable<number> {
    return this.ltp$;
  }
}

