import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-donut-score',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './donut-score.component.html',
  styleUrls: ['./donut-score.component.css']
})
export class DonutScoreComponent {
  @Input() value = 0;
  circumference = 2 * Math.PI * 50;
}
