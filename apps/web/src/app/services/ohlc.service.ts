import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { Observable } from 'rxjs';
import { Ohlc } from '../models/ohlc.model';

@Injectable({ providedIn: 'root' })
export class OhlcService {
  private http = inject(HttpClient);
  private apiBase = environment.apiBase;

  getCEPE(side: 'CE' | 'PE', gran = '1s', limit = 900): Observable<Ohlc[]> {
    const params = new HttpParams().set('side', side).set('gran', gran).set('limit', limit);
    return this.http.get<Ohlc[]>(`${this.apiBase}/ohlc/cepe`, { params });
  }
}
