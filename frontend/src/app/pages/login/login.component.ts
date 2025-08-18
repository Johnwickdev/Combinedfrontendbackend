import { Component } from '@angular/core';
import { AuthService } from '../../services/auth.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-login',
  template: `<div>Logging in...</div>`,
})
export class LoginComponent {
  constructor(private auth: AuthService, private router: Router) {
    this.autoLogin();
  }
autoLogin() {
  const currentUrl = window.location.href;
  const codeMatch = currentUrl.match(/[?&]code=([^&]+)/);

  if (codeMatch && codeMatch[1]) {
    const code = decodeURIComponent(codeMatch[1]);
    console.log('🔐 Auth code received from redirect:', code);

    this.auth.exchange(code).subscribe({
      next: () => {
        console.log('✅ Code exchanged, token saved');
        setTimeout(() => this.router.navigate(['/dashboard']), 1000);
      },
      error: err => {
        console.error('❌ Token exchange failed:', err);
      },
    });

  } else {
    console.log('📡 No code found in URL, redirecting to Upstox login...');
    this.auth.getLoginUrl().subscribe({
      next: loginUrl => {
        console.log('➡️ Redirecting to:', loginUrl);
        window.location.href = loginUrl;
      },
      error: err => {
        console.error('❌ Failed to get login URL:', err);
      }
    });
  }
}
}
