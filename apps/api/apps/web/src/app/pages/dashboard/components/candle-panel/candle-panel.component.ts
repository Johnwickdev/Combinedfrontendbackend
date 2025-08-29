import { AfterViewInit, Component, ElementRef, ViewChild } from '@angular/core';
import { CommonModule } from '@angular/common';

interface Candle {
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

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

  ngAfterViewInit() {
    this.generate();
    this.draw();
    window.addEventListener('resize', () => this.draw());
  }

  generate() {
    let price = 24835.25;
    for (let i = 0; i < 60; i++) {
      const open = price;
      const close = open + (Math.random() - 0.5) * 20;
      const high = Math.max(open, close) + Math.random() * 5;
      const low = Math.min(open, close) - Math.random() * 5;
      const volume = 100 + Math.random() * 200;
      this.candles.push({ open, high, low, close, volume });
      price = close;
    }
  }

  draw() {
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
