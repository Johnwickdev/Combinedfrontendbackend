import { inject, Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { map, Observable, timer, switchMap, shareReplay } from 'rxjs';
import { environment } from '../../environments/environment';

export interface AuthState {
  ready: boolean;
  expiresInSec: number;
}

@Injectable({ providedIn: 'root' })
export class AuthService {
  private http = inject(HttpClient);
  private baseUrl = environment.apiBaseUrl;

  /** 1️⃣  ask the backend for the login-URL */
  getLoginUrl(): Observable<string> {
    return this.http.get<{ url: string }>(`${this.baseUrl}/auth/url`).pipe(map(r => r.url));
  }

  /** 2️⃣  send the `code` that the user pasted */
  exchange(code: string): Observable<void> {
    return this.http.post<void>(`${this.baseUrl}/auth/exchange?code=${code}`, null);
  }

  /** 3️⃣  poll the backend every 3 s until it says `ready:true` */
  pollStatus(): Observable<AuthState> {
    return timer(0, 3000).pipe(
      switchMap(() => this.http.get<AuthState>(`${this.baseUrl}/auth/status`)),
      shareReplay(1)
    );
  }
}
