import { inject, Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface AuthStatus {
  connected: boolean;
  expiresAt: string | null;
  remainingSeconds: number;
}

@Injectable({ providedIn: 'root' })
export class AuthService {
  private http = inject(HttpClient);
  private apiBase = environment.apiBase;

  /** Redirect the browser to the backend login endpoint. */
  redirectToLogin() {
    window.location.href = `${this.apiBase}/auth/login`;
  }

  /** Fetch the current auth status */
  getStatus(): Observable<AuthStatus> {
    return this.http.get<AuthStatus>(`${this.apiBase}/auth/status`);
  }

  /** Send the Upstox auth code to the backend for exchange */
  exchangeCode(code: string) {
    return this.http.get<void>(`${this.apiBase}/auth/exchange`, {
      params: { code }
    });
  }
}
