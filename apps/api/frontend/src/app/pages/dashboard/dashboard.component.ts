import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css']
})
export class DashboardComponent implements OnInit {
  tokenExpiryTime: string = '00:30:00'; // dummy timer

  constructor(private router: Router) {}

  ngOnInit(): void {
    // WebSocket logic and token timer will be added here later
  }

  goToLogin(): void {
    this.router.navigate(['/login']);
  }
}
