import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { interval, Subscription } from 'rxjs';
import { finalize } from 'rxjs/operators';
import { MarketDataService } from '../../../../services/market-data.service';
import { TradeRow } from '../../../../models/trade-row';

@Component({
  selector: 'app-sector-table',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './sector-table.component.html',
  styleUrls: ['./sector-table.component.css']
})
export class SectorTableComponent implements OnInit, OnDestroy {
  selectedSide: 'both' | 'CE' | 'PE' = 'both';
  limit = 50;
  rows: TradeRow[] = [];
  loading = true;
  lastLoadedAt?: string;
  private timerSub?: Subscription;

  constructor(private marketData: MarketDataService) {}

  ngOnInit() {
    this.load();
    this.timerSub = interval(10000).subscribe(() => this.load());
  }

  ngOnDestroy() {
    this.timerSub?.unsubscribe();
  }

  load() {
    if (!this.rows.length) {
      this.loading = true;
    }
    this.marketData
      .getSectorTrades(this.limit, this.selectedSide)
      .pipe(finalize(() => (this.loading = false)))
      .subscribe({
        next: rows => {
          this.rows = rows ?? [];
          this.lastLoadedAt = new Date().toISOString();
        },
        error: () => {
          this.rows = [];
        }
      });
  }

  onSideChange(side: 'both' | 'CE' | 'PE') {
    if (this.selectedSide !== side) {
      this.selectedSide = side;
      this.rows = [];
      this.load();
    }
  }

  trackTx = (_: number, r: TradeRow) => r.txId;
}
