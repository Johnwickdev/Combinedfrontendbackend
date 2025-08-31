import { AfterViewInit, Component, ElementRef, ViewChild, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MarketDataService, Candle } from '../../../../services/market-data.service';

@Component({
  selector: 'app-candle-panel',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './candle-panel.component.html',
  styleUrls: ['./candle-panel.component.css']
})
export class CandlePanelComponent implements AfterViewInit {
  @ViewChild('canvas', { static: true }) canvas!: ElementRef<HTMLCanvasElement>;
  candles: Candle[] = [];
  private md = inject(MarketDataService);

  ngAfterViewInit() {
    this.md.getAxisBankCandles().subscribe(c => {
      this.candles = c;
      this.draw();
    });
    window.addEventListener('resize', () => this.draw());
  }

  draw() {
    if (!this.candles.length) return;
    const canvas = this.canvas.nativeElement;
    const ctx = canvas.getContext('2d')!;
    const dpr = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, width, height);
    canvas.style.cursor = 'crosshair';

    const volumeHeight = height * 0.25;
    const chartHeight = height - volumeHeight - 16;

    const highs = this.candles.map(c => c.high);
    const lows = this.candles.map(c => c.low);
    const maxPrice = Math.max(...highs);
    const minPrice = Math.min(...lows);
    const maxVol = Math.max(...this.candles.map(c => c.volume));

    ctx.strokeStyle = '#2b3044';
    ctx.lineWidth = 1;
    const gridLines = 5;
    for (let i = 0; i <= gridLines; i++) {
      const y = (chartHeight / gridLines) * i;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(width, y);
      ctx.stroke();
    }

    const barWidth = width / this.candles.length;
    const bodyWidth = barWidth * 0.5;
    const styles = getComputedStyle(document.documentElement);
    const rise = styles.getPropertyValue('--rise');
    const fall = styles.getPropertyValue('--fall');

    this.candles.forEach((c, i) => {
      const x = i * barWidth + barWidth / 2;
      const openY = chartHeight - ((c.open - minPrice) / (maxPrice - minPrice)) * chartHeight;
      const closeY = chartHeight - ((c.close - minPrice) / (maxPrice - minPrice)) * chartHeight;
      const highY = chartHeight - ((c.high - minPrice) / (maxPrice - minPrice)) * chartHeight;
      const lowY = chartHeight - ((c.low - minPrice) / (maxPrice - minPrice)) * chartHeight;
      const color = c.close >= c.open ? rise : fall;
      ctx.strokeStyle = color;
      ctx.fillStyle = color;

      ctx.beginPath();
      ctx.moveTo(x, highY);
      ctx.lineTo(x, lowY);
      ctx.stroke();

      const top = Math.min(openY, closeY);
      const bottom = Math.max(openY, closeY);
      ctx.fillRect(x - bodyWidth / 2, top, bodyWidth, bottom - top);

      const volHeight = (c.volume / maxVol) * volumeHeight;
      ctx.fillRect(x - bodyWidth / 2, chartHeight + volumeHeight - volHeight, bodyWidth, volHeight);
    });
  }
}
