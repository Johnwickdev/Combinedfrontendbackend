import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { map, shareReplay } from 'rxjs/operators';
import { Observable } from 'rxjs';

export interface Tick {
  symbol: string;
  last: number;
  time: string;
}

@Injectable({ providedIn: 'root' })
export class TickService {

  private readonly stream$: Observable<Tick>;

  constructor(private http: HttpClient) {

    // Server-Sent Events handled as a text stream
    this.stream$ = new Observable<MessageEvent>(observer => {
      const sse = new EventSource('/api/stream/ticks');   // proxy to 8081
      sse.onmessage = ev => observer.next(ev);
      sse.onerror   = err => observer.error(err);
      return () => sse.close();
    }).pipe(
      map(ev => JSON.parse(ev.data) as Tick),
      shareReplay({ bufferSize: 1, refCount: true })
    );
  }

  /** public subscription point */
  ticks(): Observable<Tick> {
    return this.stream$;
  }
}
