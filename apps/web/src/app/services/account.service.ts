import { inject, Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from '../../environments/environment';
import { map } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class AccountService {
  private http = inject(HttpClient);
  private apiBase = environment.apiBase;

  getBalance() {
    return this.http
      .get<{ balance: number }>(`${this.apiBase}/ops/upstox-balance`)
      .pipe(map(r => r.balance));
  }
}
