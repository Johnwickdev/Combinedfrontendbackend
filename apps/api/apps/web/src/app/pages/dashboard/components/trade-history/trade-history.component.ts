import { Component, Input, Output, EventEmitter } from '@angular/core';
import { CommonModule } from '@angular/common';
import { TradeRow } from '../../../../models/trade-history.model';

@Component({
  selector: 'app-trade-history',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './trade-history.component.html',
  styleUrls: ['./trade-history.component.css']
})
export class TradeHistoryComponent {
  @Input() rows: TradeRow[] = [];
  @Input() loading = false;
  @Input() filterSide: 'both' | 'CE' | 'PE' = 'both';
  @Output() filterSideChange = new EventEmitter<'both' | 'CE' | 'PE'>();
}
