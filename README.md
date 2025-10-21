# DB Client Node

A specialized node for database operations in the OBS01PY system.

## Features

- **MongoDB Operations**: Full CRUD operations
- **Data Backup**: Automatic and manual backup capabilities
- **Data Restore**: Restore from backup files
- **TTL Management**: Automatic data expiration
- **Query Processing**: Advanced data querying
- **Direct Communication**: Emergency and critical messaging

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

## Database Operations

### Insert Data
```python
# Via direct communication
node.insert_data("NodeHeartbeat", {
    "node_id": "engine_001",
    "status": "running",
    "timestamp": datetime.now()
})
```

### Query Data
```python
# Query recent heartbeat data
results = node.query_data("NodeHeartbeat", 
    {"status": "running"}, 
    limit=50
)
```

### Backup Operations
```bash
# Manual backup via UDP command
python ../../src/onboard_core/obs_client.py db_command backup_database
```

## Testing

```bash
# Unit tests
pytest tests/ -v

# Integration tests
python tests/test_db_integration.py
```

## API

### Commands
- `db_command`: Database management operations
- `query_data`: Query database collections
- `backup_database`: Trigger manual backup
- `restore_database`: Restore from backup

### Status
Returns database-specific status including:
- Connection status
- Backup status
- TTL settings
- Database statistics