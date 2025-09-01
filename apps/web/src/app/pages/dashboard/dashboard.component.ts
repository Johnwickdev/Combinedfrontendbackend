import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { TopbarComponent } from './components/topbar/topbar.component';
import { SideRailComponent } from './components/side-rail/side-rail.component';
import { AssetHeaderComponent } from './components/asset-header/asset-header.component';
import { MetricCardComponent } from './components/metric-card/metric-card.component';
import { CandlePanelComponent } from './components/candle-panel/candle-panel.component';
import { DonutScoreComponent } from './components/donut-score/donut-score.component';
import { TrustBarComponent } from './components/trust-bar/trust-bar.component';
import { SectorTradesComponent } from './sector-trades.component';
import { AuthService } from '../../services/auth.service';
import { formatCountdown } from '../../utils/time';
import { AccountService } from '../../services/account.service';
import { DataSourceFacade } from '../../services/data-source.facade';
import { Observable } from 'rxjs';
import { Ohlc } from '../../models/ohlc.model';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [
    CommonModule,
    TopbarComponent,
    SideRailComponent,
    AssetHeaderComponent,
    MetricCardComponent,
    CandlePanelComponent,
    DonutScoreComponent,
    TrustBarComponent,
    SectorTradesComponent
  ],
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css']
})
export class DashboardComponent implements OnInit, OnDestroy {
  metrics = [
    { title: 'Balance', value: '₹ 0.00' },
    { title: 'Daily Volume', value: '₹ 2,372,139.74' },
    { title: "Open Interest ('000)", value: '120.6' },
    { title: "Lots Traded ('000)", value: '271.35' }
  ];

  connected = false;
  expiresAt: string | null = null;
  remaining = 0;
  polling: any;
  private countdown: any;

  private auth = inject(AuthService);
  private account = inject(AccountService);
  readonly facade = inject(DataSourceFacade);

  ngOnInit() {
    this.checkStatus();
    this.polling = setInterval(() => this.checkStatus(), 15000);
    this.countdown = setInterval(() => {
      if (this.connected && this.remaining > 0) {
        this.remaining--;
        if (this.remaining <= 0) {
          this.connected = false;
        }
      }
    }, 1000);
    this.facade.init();
  }

  ngOnDestroy() {
    clearInterval(this.polling);
    clearInterval(this.countdown);
  }

  private checkStatus() {
    this.auth.getStatus().subscribe({
      next: s => {
        this.connected = s.connected;
        this.expiresAt = s.expiresAt;
        this.remaining = s.remainingSeconds;
        if (this.connected) {
          this.account.getBalance().subscribe(b => {
            this.metrics[0].value = '₹ ' + b.toFixed(2);
          });
        } else {
          this.metrics[0].value = '₹ 0.00';
        }
      }
    });
  }

  login() {
    this.auth.getLoginUrl().subscribe({ next: url => (window.location.href = url) });
  }

  refresh() {
    this.login();
  }

  formatRemaining() {
    return formatCountdown(this.remaining);
  }

  get mode$(): Observable<'LIVE' | 'HISTORIC'> { return this.facade.mode$; }
  get lastTickAge$(): Observable<number> { return this.facade.lastTickAge$; }
  get futLtp$(): Observable<number | null> { return this.facade.futLtp$; }
  get ceOhlc$(): Observable<Ohlc[]> { return this.facade.ceOhlc$; }
  get peOhlc$(): Observable<Ohlc[]> { return this.facade.peOhlc$; }

}
