#!/usr/bin/env python3
"""
Redis Monitor Logger
Connects to a Redis instance, monitors commands, and logs them to CSV
with client name mapping from CLIENT LIST.
"""

import redis
import argparse
import csv
import threading
import time
import re
import socket
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import signal
import sys


class RedisMonitorLogger:
    """Captures Redis MONITOR output with client name mapping."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        password: Optional[str] = None,
        output_file: str = "redis_monitor.csv",
        output_dir: Optional[str] = None,
        client_list_interval: int = 10,
    ):
        self.host = host
        self.port = port
        self.password = password
        
        # Handle output directory
        if output_dir:
            output_dir_path = Path(output_dir)
            output_dir_path.mkdir(parents=True, exist_ok=True)
            self.output_file_base = output_dir_path / Path(output_file).name
        else:
            self.output_file_base = Path(output_file)
        
        self.client_list_interval = client_list_interval
        self.current_hour = None
        self.client_name = f"Redis-Monitor-{socket.gethostname()}"
        
        self.redis_conn = None
        self.monitor_conn = None
        self.client_names: Dict[str, str] = {}
        self.active = True
        self.lock = threading.Lock()
        self.monitor_thread = None
        self.client_list_thread = None
        
        # CSV setup
        self.csv_file = None
        self.csv_writer = None
        self.output_file = None
        self._rotate_log_file()
        
        print(f"Redis Monitor Logger initialized for {host}:{port}")

    def _get_hourly_filename(self) -> Path:
        """Generate filename with hourly timestamp."""
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H0000")
        
        # Insert timestamp before file extension
        base = self.output_file_base.stem
        ext = self.output_file_base.suffix
        parent = self.output_file_base.parent
        
        return parent / f"{base}_{timestamp}{ext}"
    
    def _rotate_log_file(self):
        """Rotate log file if hour has changed."""
        now = datetime.now()
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        
        # Check if we need to rotate
        if self.current_hour == current_hour and self.csv_file and not self.csv_file.closed:
            return
        
        # Close existing file
        if self.csv_file and not self.csv_file.closed:
            try:
                self.csv_file.flush()
                self.csv_file.close()
            except Exception as e:
                print(f"Error closing old log file: {e}")
        
        # Update current hour
        self.current_hour = current_hour
        
        # Generate new filename
        self.output_file = self._get_hourly_filename()
        
        # Check if file exists
        file_exists = self.output_file.exists() and self.output_file.stat().st_size > 0
        
        # Open new file
        self.csv_file = open(self.output_file, 'a', newline='', buffering=1)
        self.csv_writer = csv.DictWriter(
            self.csv_file,
            fieldnames=[
                'timestamp', 'server_ip', 'server_port', 'client_ip', 
                'client_port', 'client_name', 'command', 'args'
            ]
        )
        
        # Write header if new file
        if not file_exists:
            self.csv_writer.writeheader()
            self.csv_file.flush()
        
        print(f"Logging to: {self.output_file}")

    def _connect(self) -> bool:
        """Connect to Redis instance."""
        try:
            self.redis_conn = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                decode_responses=True,
                socket_keepalive=True,
                health_check_interval=30,
            )
            self.redis_conn.ping()
            
            # Set client name
            self.redis_conn.client_setname(self.client_name)
            
            print(f"Connected to Redis at {self.host}:{self.port}")
            print(f"Client name set to: {self.client_name}")
            return True
        except Exception as e:
            print(f"Failed to connect to Redis: {e}")
            return False

    def _update_client_list(self):
        """Periodically update client name mapping from CLIENT LIST."""
        while self.active:
            try:
                if not self.redis_conn:
                    time.sleep(self.client_list_interval)
                    continue
                
                client_list = self.redis_conn.execute_command("CLIENT", "LIST")
                client_names = {}
                
                for line in client_list.split("\n"):
                    if not line.strip():
                        continue
                    
                    # Parse client list line into key=value pairs
                    parts = {}
                    for part in line.split():
                        if "=" in part:
                            key, value = part.split("=", 1)
                            parts[key] = value
                    
                    if "addr" in parts:
                        addr = parts["addr"]
                        # Get client name, default to "Unknown" if empty or missing
                        client_name = parts.get("name", "")
                        if not client_name:
                            client_name = "Unknown"
                        client_names[addr] = client_name
                
                with self.lock:
                    self.client_names = client_names
                
                print(f"Updated client list: {len(client_names)} clients")
                
            except Exception as e:
                print(f"Error updating client list: {e}")
            
            time.sleep(self.client_list_interval)

    def _parse_monitor_line(self, line: str) -> Optional[Dict]:
        """
        Parse Redis MONITOR output line.
        Format: timestamp [db client_addr] "command" "arg1" "arg2" ...
        """
        try:
            # Extract timestamp
            parts = line.split(maxsplit=1)
            if len(parts) < 2:
                return None
            
            timestamp = float(parts[0])
            rest = parts[1]
            
            # Extract [db client_addr]
            bracket_match = re.search(r'\[(.*?)\]', rest)
            if not bracket_match:
                return None
            
            db_client = bracket_match.group(1)
            db_parts = db_client.split(maxsplit=1)
            
            client_addr = db_parts[1] if len(db_parts) > 1 else "unknown"
            
            # Parse client IP and port
            client_ip, client_port = self._parse_client_addr(client_addr)
            
            # Get client name from mapping
            with self.lock:
                client_name = self.client_names.get(client_addr, "Unknown")
            
            # Extract command and args (after the bracket)
            command_part = rest[bracket_match.end():].strip()
            command_args = self._parse_command_args(command_part)
            
            if not command_args:
                command = ""
                args = ""
            else:
                command = command_args[0].upper()
                args = " ".join(command_args[1:])
            
            # Format timestamp
            dt = datetime.fromtimestamp(timestamp)
            timestamp_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            return {
                "timestamp": timestamp_str,
                "server_ip": self.host,
                "server_port": self.port,
                "client_ip": client_ip,
                "client_port": client_port,
                "client_name": client_name,
                "command": command,
                "args": args,
            }
            
        except Exception as e:
            print(f"Error parsing monitor line: {e}")
            return None

    def _parse_client_addr(self, addr: str) -> tuple:
        """Parse client address to (ip, port)."""
        addr = addr.strip()
        
        # Handle IPv6 [::1]:port
        ipv6_match = re.match(r'^\[(.+)\]:(\d+)$', addr)
        if ipv6_match:
            ip, port = ipv6_match.group(1), ipv6_match.group(2)
        else:
            # Handle IPv4 ip:port
            ipv4_match = re.match(r'^(.+?):(\d+)$', addr)
            if ipv4_match:
                ip, port = ipv4_match.group(1), ipv4_match.group(2)
            else:
                return addr, "unknown"
        
        # Normalize localhost
        if ip == "::1":
            ip = "127.0.0.1"
        
        return ip, port

    def _parse_command_args(self, command_str: str) -> list:
        """Parse command arguments from quoted strings."""
        args = []
        # Simple regex to match quoted strings
        for match in re.finditer(r'"([^"]*)"', command_str):
            args.append(match.group(1))
        return args

    def _monitor_loop(self):
        """Main monitoring loop."""
        backoff = 1
        max_backoff = 60
        
        while self.active:
            try:
                # Create a separate connection for MONITOR
                self.monitor_conn = redis.Redis(
                    host=self.host,
                    port=self.port,
                    password=self.password,
                    decode_responses=True,
                )
                
                # Set client name for monitor connection
                self.monitor_conn.client_setname(self.client_name)
                
                print("Starting MONITOR command...")
                raw_conn = self.monitor_conn.connection_pool.get_connection()
                
                try:
                    raw_conn.send_command("MONITOR")
                    
                    while self.active:
                        line = raw_conn.read_response(disconnect_on_error=True)
                        
                        if line:
                            # Check if we need to rotate the log file
                            self._rotate_log_file()
                            
                            log_entry = self._parse_monitor_line(str(line))
                            
                            if log_entry and self.csv_file and not self.csv_file.closed:
                                self.csv_writer.writerow(log_entry)
                                self.csv_file.flush()
                    
                finally:
                    self.monitor_conn.connection_pool.release(raw_conn)
                    
            except redis.ConnectionError as e:
                print(f"Connection lost: {e}. Reconnecting in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                
            except Exception as e:
                print(f"Error in monitor loop: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    def start(self):
        """Start the monitor logger."""
        print("Starting Redis Monitor Logger...")
        
        if not self._connect():
            print("Failed to connect to Redis. Exiting.")
            return False
        
        # Start client list updater thread
        self.client_list_thread = threading.Thread(
            target=self._update_client_list,
            daemon=True,
            name="client-list-updater"
        )
        self.client_list_thread.start()
        
        # Start monitor thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True,
            name="monitor-loop"
        )
        self.monitor_thread.start()
        
        print(f"Monitor logger started. Log rotation: hourly")
        print(f"Client list refresh interval: {self.client_list_interval}s")
        return True

    def stop(self):
        """Stop the monitor logger gracefully."""
        print("\nStopping Redis Monitor Logger...")
        self.active = False
        
        # Wait for threads to finish (with timeout)
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2.0)
        
        if self.client_list_thread and self.client_list_thread.is_alive():
            self.client_list_thread.join(timeout=2.0)
        
        # Close connections
        if self.redis_conn:
            try:
                self.redis_conn.close()
            except Exception:
                pass
        
        if self.monitor_conn:
            try:
                self.monitor_conn.close()
            except Exception:
                pass
        
        # Close CSV file after threads have stopped
        if self.csv_file and not self.csv_file.closed:
            try:
                self.csv_file.flush()
                self.csv_file.close()
            except Exception:
                pass
        
        print("Monitor logger stopped.")


def main():
    parser = argparse.ArgumentParser(
        description="Redis Monitor Logger - Capture and log Redis commands with client info"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Redis host (default: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6379,
        help="Redis port (default: 6379)"
    )
    parser.add_argument(
        "--password",
        help="Redis password (optional)"
    )
    parser.add_argument(
        "--output",
        default="redis_monitor.csv",
        help="Output CSV file name (default: redis_monitor.csv)"
    )
    parser.add_argument(
        "--output-dir",
        help="Directory to write CSV files to (created if doesn't exist)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=10,
        help="Client list refresh interval in seconds (default: 10)"
    )
    
    args = parser.parse_args()
    
    # Create logger instance
    logger = RedisMonitorLogger(
        host=args.host,
        port=args.port,
        password=args.password,
        output_file=args.output,
        output_dir=args.output_dir,
        client_list_interval=args.interval,
    )
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start logger
    if logger.start():
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.stop()
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
