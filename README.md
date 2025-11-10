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
- `query_data`: Query documents with FILTER and SORT support
- `create_collection`: Create a new collection
- `drop_collection`: Delete a collection
- `get_stats`: Get database statistics
- `backup_database`: Trigger manual database backup
- `restore_database`: Restore database from backup

### Data Flow:
```
Master Core → IPC Command → DB Client → MongoDB → Response → Master Core
```

## TTL Management

The DB Client automatically adds TTL (Time To Live) expiration to all documents using MongoDB's standard TTL pattern:

### Automatic TTL:
- **Default TTL**: 7 days (configurable via `data_ttl_days`)
- **Automatic Cleanup**: MongoDB automatically removes expired documents
- **TTL Index**: Automatic creation of TTL indexes on `created_at` field (MongoDB standard)
- **Field Used**: `created_at` (datetime field added automatically on insert)

### TTL Configuration:
```json
{
    "data_ttl_days": 7,  // Default TTL for all documents (in days)
    // For testing with short TTL:
    // "data_ttl_days": 0.000116,  // 10 seconds
    // "data_ttl_days": 0.000694,  // 60 seconds (1 minute)
}
```

**TTL Testing Values:**
- `0.000116` days = 10 seconds
- `0.000694` days = 60 seconds (1 minute)
- `0.001` days = 86.4 seconds (~1.4 minutes)

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
            "timestamp": "2025-10-22T20:53:52.270164"
            // Note: created_at field is automatically added by db-client-node
        }
    }
}
```

### Query Data (with FILTER and SORT)
```python
# Query with filter and sort
query_message = {
    "message_id": "unique-id",
    "type": "command",
    "priority": 2,
    "source": "master_core",
    "destination": "db_client",
    "payload": {
        "command": "query_data",
        "collection": "System",
        "query": {"status": "ONLINE"},  # MongoDB filter
        "sort": [("startuptime", -1)],   # List of (field, direction) tuples
        "limit": 50,
        "skip": 0
    },
    "timestamp": 1720100000.0,
    "requires_ack": False
}
```

**Query Parameters:**
- `collection` (required): Collection name to query
- `query` (optional): MongoDB filter dictionary (default: {})
- `sort` (optional): List of tuples `[("field", direction)]` where direction is 1 (asc) or -1 (desc)
- `limit` (optional): Maximum documents to return (default: 100)
- `skip` (optional): Number of documents to skip for pagination (default: 0)

**Response Format:**
```python
{
    "message_id": "response-id",
    "type": "response",
    "source": "db_client",
    "destination": "requester",
    "payload": {
        "status": "success",  # or "error"
        "collection": "System",
        "query_results": [...],  # List of documents
        "count": 5,
        "query_params": {
            "filter": {...},
            "sort": [...],
            "limit": 50,
            "skip": 0
        }
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