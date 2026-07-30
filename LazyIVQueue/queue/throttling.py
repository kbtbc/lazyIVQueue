"""
Throttling logging utility for LazyIVQueue.
This module provides functions to log circuit breaker and throttling events
to a separate file with timestamps and key statistics.
"""

import json
import time
from typing import Dict, Any

def log_throttling_event(event_type: str, status: str, manager, **kwargs) -> None:
    """
    Log throttling events to a separate file with timestamp and key stats.
    
    Args:
        event_type (str): Type of event (e.g., "CIRCUIT_BREAKER_PAUSED", "RECOVERING")
        status (str): Current tuning status
        manager: IVQueueManager instance for accessing stats
        **kwargs: Additional event-specific data to log
    """
    try:
        # Read existing log entries
        try:
            with open("throttling.log", "r") as f:
                log_entries = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            log_entries = []
        
        # Create new log entry using manager's properties
        entry = {
            "timestamp": time.time(),
            "event": event_type,
            "status": status,
            "pending_count": manager.pending_count,
            "awaiting_iv_count": manager.awaiting_iv_count,
            "scout_percent": manager._current_scout_percent,
            "throttled_step": manager._throttled_step,
            "baseline_scout_percent": manager._baseline_scout_percent(),
            "concurrency": manager._current_concurrency,
            "manual_pause": manager._manual_pause,
            "pause_reason": getattr(manager, '_pause_reason', ''),
            "total_pauses_triggered": manager._total_pauses_triggered
        }
        
        # Add any additional key metrics from kwargs
        entry.update(kwargs)
        
        # Add to log entries
        log_entries.append(entry)
        
        # Write back to file
        with open("throttling.log", "w") as f:
            json.dump(log_entries, f, indent=2)
    except Exception as e:
        # Silently fail to avoid breaking the main application
        pass

# Initialize throttling log file
def init_throttling_log() -> None:
    """Initialize the throttling log file."""
    try:
        with open("throttling.log", "w") as f:
            json.dump([], f)
    except Exception as e:
        print(f"Failed to initialize throttling log: {e}")