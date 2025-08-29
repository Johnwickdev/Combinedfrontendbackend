import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { interval, Subscription } from 'rxjs';
import { MarketDataService, SectorTradeRow } from '../../services/market-data.service';

type Side = 'both' | 'CE' | 'PE';

@Component({
  selector: 'app-sector-trades',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './sector-trades.component.html',
  styleUrls: ['./sector-trades.component.css']
})
export class SectorTradesComponent implements OnInit, OnDestroy {
  side: Side = 'both';
  rows: SectorTradeRow[] = [];
  loading = false;
  error?: string;
  source?: 'live' | 'influx';

  private timer?: Subscription;

  constructor(private marketData: MarketDataService) {}

  ngOnInit() {
    const stored = localStorage.getItem('sector.side') as Side | null;
    if (stored === 'CE' || stored === 'PE' || stored === 'both') {
      this.side = stored;
    }
    this.fetch(this.side);
    this.timer = interval(5000).subscribe(() => this.fetch(this.side));
  }

  ngOnDestroy() {
    this.timer?.unsubscribe();
  }

  setSide(side: Side) {
    if (side === this.side) return;
    this.side = side;
    localStorage.setItem('sector.side', side);
    this.fetch(side);
  }

  fetch(side: Side) {
    this.loading = true;
    this.error = undefined;
    this.marketData.getSectorTrades(side).subscribe({
      next: res => {
        this.rows = res.rows;
        this.source = res.source;
        this.loading = false;
      },
      error: () => {
        this.error = 'Failed to load';
        this.rows = [];
        this.loading = false;
      }
    });
  }

  trackByTs = (_: number, r: SectorTradeRow) => r.ts + r.optionType + r.strike;
}

