import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { Observable } from 'rxjs';
import { MarketStatus } from '../models/market-status.model';

@Injectable({ providedIn: 'root' })
export class MarketService {
  private http = inject(HttpClient);
  private apiBase = environment.apiBase;

  getStatus(): Observable<MarketStatus> {
    return this.http.get<MarketStatus>(`${this.apiBase}/market/status`);
    }
}
