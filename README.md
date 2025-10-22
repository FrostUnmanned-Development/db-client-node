# DB Client Node

A specialized node for database operations and data management in the OBS01PY system.

## Features

- **MongoDB Operations**: Full CRUD operations with connection pooling
- **IPC Communication**: Seamless integration with Master Core and other nodes via standardized IPC
- **TTL Management**: Automatic data expiration and cleanup (configurable TTL)
- **Data Backup**: Automatic and manual backup capabilities
- **Data Restore**: Restore from backup files
- **Query Processing**: Advanced data querying with filtering and aggregation
- **Direct Communication**: Emergency and critical messaging capabilities
- **Collection Management**: Automatic collection creation and indexing

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Edit `config.json` to configure:
- MongoDB connection settings
- Database name and TTL settings
- Backup configuration
- Node communication ports
- Master Core IPC connection

```json
{
    "mongodb_host": "localhost",
    "mongodb_port": 27017,
    "database_name": "OBSDB",
    "data_ttl_days": 7,
    "node_port": 14552,
    "master_core_host": "localhost",
    "master_core_port": 14551,
    "backup_enabled": true,
    "backup_interval_hours": 24,
    "backup_retention_days": 30
}
```

## Usage

### Run as standalone node:
```bash
python src/db_client/db_client_node.py
```

### Run as daemon:
```bash
python src/db_client/db_client_node.py --daemon
```

### With custom config:
```bash
python src/db_client/db_client_node.py --config my_config.json
```

## IPC Communication

The DB Client communicates with other nodes via standardized IPC messages:

### Incoming Commands:
- `insert_one`: Insert a single document into specified collection
- `insert_many`: Insert multiple documents into specified collection
- `find_one`: Find a single document with optional filtering
- `find_many`: Find multiple documents with filtering and pagination
- `update_one`: Update a single document
- `update_many`: Update multiple documents
- `delete_one`: Delete a single document
- `delete_many`: Delete multiple documents
- `backup_database`: Trigger manual database backup
- `restore_database`: Restore database from backup

### Data Flow:
```
Master Core → IPC Command → DB Client → MongoDB → Response → Master Core
```

## TTL Management

The DB Client automatically adds TTL (Time To Live) expiration to all documents:

### Automatic TTL:
- **Default TTL**: 7 days (configurable)
- **Automatic Cleanup**: MongoDB automatically removes expired documents
- **TTL Index**: Automatic creation of TTL indexes on `ttl_expiration` field

### TTL Configuration:
```json
{
    "data_ttl_days": 7,  // Default TTL for all documents
    "collection_ttl": {   // Per-collection TTL overrides
        "system": 30,     // System logs kept for 30 days
        "can_data": 7,    // CAN data kept for 7 days
        "emergency": 90   // Emergency logs kept for 90 days
    }
}
```

## Database Operations

### Insert Data (via IPC)
```python
# Master Core sends IPC command to DB Client
ipc_message = {
    "type": "command",
    "payload": {
        "command": "insert_one",
        "collection": "Unknown",
        "data": {
            "pgn": 127245,
            "source": 222,
            "dest": 255,
            "fields": [5, None, 31, 0.163, -0.0001, 65535],
            "timestamp": "2025-10-22T20:53:52.270164",
            "ttl_expiration": "2025-10-29T20:53:52.270344"
        }
    }
}
```

### Query Data
```python
# Query recent heartbeat data
query_message = {
    "type": "command",
    "payload": {
        "command": "find_many",
        "collection": "NodeHeartbeat",
        "filter": {"status": "running"},
        "limit": 50,
        "sort": [("timestamp", -1)]
    }
}
```

### Backup Operations
```bash
# Manual backup via UDP command (from main OBS01PY directory)
python ../../src/onboard_core/obs_client.py db_command backup_database
```

## Collections

The DB Client manages several collections:

- **Unknown**: Unrecognized CAN messages and PGNs
- **HEARTBEAT**: System health and status messages
- **ENGINE**: Engine parameters and diagnostics
- **NAVIGATION**: Position, heading, and navigation data
- **FUEL**: Fuel consumption and tank levels
- **ENERGYDISTRIBUTION**: Power management data
- **system**: System logs and node status

## Testing

```bash
# Unit tests
pytest tests/ -v

# Integration tests
python tests/test_db_integration.py

# Test MongoDB connection
python -c "
import pymongo
client = pymongo.MongoClient('localhost', 27017)
print('MongoDB connection:', client.admin.command('ping'))
"
```

## API

### Commands
- `db_command`: Database management operations
- `query_data`: Query database collections
- `backup_database`: Trigger manual backup
- `restore_database`: Restore from backup

### Status
Returns database-specific status including:
- MongoDB connection status
- Database statistics
- Backup status and schedule
- TTL settings and active indexes
- Collection counts and sizes
- Recent operation metrics