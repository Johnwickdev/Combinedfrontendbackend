import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-asset-header',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './asset-header.component.html',
  styleUrls: ['./asset-header.component.css']
})
export class AssetHeaderComponent {
  pair = 'NIFTY / Options';
  network = 'UPSTOX';
}
