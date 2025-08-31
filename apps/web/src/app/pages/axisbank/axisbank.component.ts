import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SideRailComponent } from '../dashboard/components/side-rail/side-rail.component';
import { CandlePanelComponent } from '../dashboard/components/candle-panel/candle-panel.component';

@Component({
  selector: 'app-axisbank',
  standalone: true,
  imports: [CommonModule, SideRailComponent, CandlePanelComponent],
  templateUrl: './axisbank.component.html',
  styleUrls: ['./axisbank.component.css']
})
export class AxisBankComponent {}
