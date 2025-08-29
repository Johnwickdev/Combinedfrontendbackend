import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';

@Component({
  selector: 'app-trust-bar',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './trust-bar.component.html',
  styleUrls: ['./trust-bar.component.css']
})
export class TrustBarComponent {
  @Input() value = 0;
}
