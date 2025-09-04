import { Component } from '@angular/core';

@Component({
  selector: 'app-login',
  templateUrl: './login.component.html',
  styleUrls: ['./login.component.css']
})
export class LoginComponent {
  /** Direct Upstox authorization URL */
  private readonly upstoxLoginUrl =
    'https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id=97fde129-556b-4a84-9083-9fea5eb53fb0&redirect_uri=https%3A%2F%2Fcombinedfrontendbackend-production.up.railway.app%2Fauth&state=botInit&scope=profile%20marketdata';

  /** Redirect the user to the Upstox login page */
  login() {
    window.location.href = this.upstoxLoginUrl;
  }
}
