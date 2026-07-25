# Schedule Optimization

Exactly 4 runs/day, min 2h gap, max 30 min shift per cycle. Score each run time against activity model (-2 to +2 per slot, max +8). If score >= 6: no change. If < 6: find candidate maximizing score, shift toward target. Update cron registration.

Confidence gate: high = optimize freely, med = only if score <= 2, low = hold.