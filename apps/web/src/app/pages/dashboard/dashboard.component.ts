import { Component, OnInit, OnDestroy } from '@angular/core';
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
import { MarketDataService } from '../../services/market-data.service';
import { Subscription } from 'rxjs';

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
    { title: 'Total Liquidity', value: '₹ 2,803,805.50' },
    { title: 'Daily Volume', value: '₹ 2,372,139.74' },
    { title: "Open Interest ('000)", value: '120.6' },
    { title: "Lots Traded ('000)", value: '271.35' }
  ];

  connected = false;
  expiresAt: string | null = null;
  remaining = 0;
  polling: any;
  private countdown: any;
  private ltpInterval: any;
  nowLtp: number | null = null;
  ltpTs?: string;
  ltpSource?: 'live' | 'influx';
  marketOpen?: boolean;
  mainInstrument: string | null = null;

  private tickSub?: Subscription;

  constructor(private auth: AuthService, private marketData: MarketDataService) {}

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
    const stored = localStorage.getItem('mainInstrumentKey');
    if (stored) {
      this.initializeInstrument(stored);
    } else {
      this.marketData.getSelection().subscribe({
        next: sel => {
          if (sel?.mainInstrument) {
            localStorage.setItem('mainInstrumentKey', sel.mainInstrument);
            this.initializeInstrument(sel.mainInstrument);
          } else {
            this.initializeInstrument('NSE_FO|64103');
          }
        },
        error: () => this.initializeInstrument('NSE_FO|64103'),
      });
    }
  }

  ngOnDestroy() {
    clearInterval(this.polling);
    clearInterval(this.countdown);
    clearInterval(this.ltpInterval);
    this.tickSub?.unsubscribe();
  }

  private checkStatus() {
    this.auth.getStatus().subscribe({
      next: s => {
        this.connected = s.connected;
        this.expiresAt = s.expiresAt;
        this.remaining = s.remainingSeconds;
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

  private initializeInstrument(key: string) {
    this.mainInstrument = key;
    this.startLtpPolling();
    this.tickSub = this.marketData.listenTicks().subscribe(tick => {
      if (tick.instrumentKey === this.mainInstrument && tick.ltp != null) {
        this.nowLtp = tick.ltp;
        this.ltpSource = 'live';
        this.ltpTs = new Date().toISOString();
      }
    });
  }

  private startLtpPolling() {
    const load = () => {
      if (!this.mainInstrument) return;
      this.marketData.getLtp(this.mainInstrument).subscribe({
        next: r => {
          if (!r || r.ltp == null) {
            this.nowLtp = null;
            this.ltpSource = undefined;
            this.ltpTs = undefined;
            return;
          }
          this.nowLtp = r.ltp;
          this.ltpSource = r.source;
          this.ltpTs = r.ts;
        },
        error: () => {
          this.nowLtp = null;
          this.ltpSource = undefined;
          this.ltpTs = undefined;
        },
      });
    };
    load();
    this.ltpInterval = setInterval(load, 5000);
  }


}
