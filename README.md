# FalkorDB Monitor Logger

A Python utility that connects to a Redis/FalkorDB instance, monitors all executed commands in real-time, and logs them to CSV files with automatic hourly rotation and client identification.

## Features

- **Real-time Command Monitoring**: Uses Redis `MONITOR` command to capture all commands executed on the server
- **Client Name Resolution**: Automatically maps client connections to their names using `CLIENT LIST`
- **Hourly Log Rotation**: Automatically creates new CSV files every hour with timestamped filenames
- **Detailed Logging**: Captures timestamp, server info, client info, command, and arguments
- **Automatic Reconnection**: Handles connection failures with exponential backoff
- **Graceful Shutdown**: Properly handles SIGINT and SIGTERM signals
- **Thread-safe**: Uses separate threads for monitoring and client list updates

## Requirements

- Python 3.6+
- `redis-py` library

Install dependencies:
```bash
pip install redis
```

## Usage

### Basic Usage

Monitor a local Redis instance:
```bash
python redis-monitor-logger.py
```

### Command-Line Options

```bash
python redis-monitor-logger.py --host <host> --port <port> [options]
```

**Available Options:**

- `--host`: Redis server hostname or IP address (default: `localhost`)
- `--port`: Redis server port (default: `6379`)
- `--password`: Redis password for authentication (optional)
- `--output`: Base name for output CSV files (default: `redis_monitor.csv`)
- `--output-dir`: Directory to write CSV files to (created if it doesn't exist)
- `--interval`: Client list refresh interval in seconds (default: `10`)

### Examples

Monitor a remote Redis instance with authentication:
```bash
python redis-monitor-logger.py --host redis.example.com --port 6379 --password mypassword
```

Monitor with custom output file and faster client list updates:
```bash
python redis-monitor-logger.py --output monitor.csv --output-dir logs --interval 5
```

Monitor with output directory (directory will be created if it doesn't exist):
```bash
python redis-monitor-logger.py --output-dir /var/log/redis-monitor
```

## Output Format

### CSV File Structure

The logger creates CSV files with the following columns:

| Column | Description |
|--------|-------------|
| `timestamp` | Command execution time (format: `YYYY-MM-DD HH:MM:SS.mmm`) |
| `server_ip` | Redis server IP/hostname |
| `server_port` | Redis server port |
| `client_ip` | Client IP address (IPv6 `::1` normalized to `127.0.0.1`) |
| `client_port` | Client connection port |
| `client_name` | Client name (from `CLIENT SETNAME`, or "Unknown") |
| `command` | Redis command (uppercase) |
| `args` | Command arguments (space-separated) |

### File Naming Convention

Log files are automatically rotated every hour with the following naming pattern:
```
{base_name}_{YYYYMMDD_HH0000}.csv
```

**Examples:**
- `redis_monitor_20251126_140000.csv` (2:00 PM - 2:59 PM)
- `redis_monitor_20251126_150000.csv` (3:00 PM - 3:59 PM)

## How It Works

### Architecture

The logger uses a multi-threaded architecture:

1. **Main Thread**: Handles initialization and graceful shutdown
2. **Monitor Thread**: Maintains the Redis `MONITOR` connection and logs commands
3. **Client List Thread**: Periodically fetches client information using `CLIENT LIST`

### Client Name Mapping

The logger maintains a mapping of client addresses to client names:
- Refreshes every `--interval` seconds (default: 10s)
- Uses `CLIENT LIST` to get current connections
- Falls back to "Unknown" for unnamed clients

### Connection Management

- Automatically reconnects on connection loss
- Uses exponential backoff (1s → 60s max)
- Maintains connection health with keepalive and health checks
- Sets client name to `Redis-Monitor-{hostname}` for identification

### Log Rotation

- Checks hourly boundary on each logged command
- Automatically closes current file and opens new one
- Appends to existing files if restarted mid-hour
- Writes CSV header only for new files

## Monitoring Considerations

⚠️ **Performance Impact**: The Redis `MONITOR` command has performance implications:
- It streams all commands to connected clients
- Can impact Redis performance on high-traffic servers
- Should be used carefully in production environments

**Best Practices:**
- Use on development/staging environments
- Monitor for short durations on production
- Consider using Redis Slow Log for production monitoring
- Ensure sufficient disk space for log files

## Signal Handling

The logger handles the following signals gracefully:
- `SIGINT` (Ctrl+C): Stops monitoring and closes files
- `SIGTERM`: Graceful shutdown for daemon mode

## Example Output

```csv
timestamp,server_ip,server_port,client_ip,client_port,client_name,command,args
2025-11-26 14:32:15.123,localhost,6379,127.0.0.1,54321,my-app,SET,mykey myvalue
2025-11-26 14:32:15.456,localhost,6379,127.0.0.1,54321,my-app,GET,mykey
2025-11-26 14:32:16.789,localhost,6379,127.0.0.1,54322,worker-1,GRAPH.QUERY,social CREATE (p:Person {name: 'Alice'})
```

## Troubleshooting

### Connection Issues

If you see "Failed to connect to Redis":
- Verify Redis is running: `redis-cli ping`
- Check host and port are correct
- Verify password if authentication is enabled
- Check network connectivity and firewall rules

### Permission Issues

If you see permission errors writing files:
- Ensure write permissions in the output directory
- Check disk space availability
- Verify the output path is valid

### Missing Client Names

If client names show as "Unknown":
- Clients must use `CLIENT SETNAME` to set their name
- Check `--interval` setting (may need faster refresh)
- Verify clients maintain persistent connections

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please submit issues and pull requests to the [GitHub repository](https://github.com/FalkorDB/falkordb-monitor-logger).
