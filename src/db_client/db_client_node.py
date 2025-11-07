#!/usr/bin/env python3
"""
DB Client Node - Specialized node for database operations
Extends BaseNode with database-specific functionality
"""

import pymongo
import threading
import time
import logging
import datetime
import uuid
from pathlib import Path
from typing import Dict, Any, Optional, List
import json
import os
import sys

# Import BaseNode from template-node submodule
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "nodes" / "template-node" / "src"))

from template_node.base_node import BaseNode, MessageType, Priority, NodeMessage

logger = logging.getLogger(__name__)

class DBClientNode(BaseNode):
    """
    DB Client Node with database operations and direct communication
    
    Features:
    - MongoDB operations (CRUD)
    - Data backup and restore
    - Query processing
    - TTL management
    - Data streaming to other nodes
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("db_client", config)
        
        # Database configuration
        self.mongodb_host = config.get("mongodb_host", "localhost")
        self.mongodb_port = config.get("mongodb_port", 27017)
        self.database_name = config.get("database_name", "OBSDB")
        
        # Database connection
        self.client = None
        self.database = None
        self.connected = False
        
        # Data management
        self.data_ttl_days = config.get("data_ttl_days", 7)
        self.backup_enabled = config.get("backup_enabled", True)
        self.backup_interval = config.get("backup_interval", 3600)  # 1 hour
        
        # Background tasks
        self.backup_thread = None
        self.backup_running = False
        
        # Register DB-specific handlers
        self.register_handler("command", self._handle_db_command)
        self.register_handler("query_data", self._handle_query_data)
        self.register_handler("backup_database", self._handle_backup)
        self.register_handler("restore_database", self._handle_restore)
        
        logger.info(f"DB Client Node initialized for {self.mongodb_host}:{self.mongodb_port}")
    
    def start(self):
        """Start the DB client node"""
        if not super().start():
            return False
        
        # Connect to database
        if self._connect_database():
            self.status = "RUNNING"
            logger.info("DB Client Node started successfully")
            return True
        else:
            self.status = "ERROR"
            return False
    
    def stop(self):
        """Stop the DB client node"""
        self._stop_backup()
        self._disconnect_database()
        super().stop()
    
    def run_daemon(self):
        """Main daemon loop for DB client"""
        logger.info("DB Client Daemon started successfully")
        try:
            while self.listening:
                time.sleep(1)  # Keep the daemon alive
        except KeyboardInterrupt:
            logger.info("Shutdown signal received")
        finally:
            self.stop()
            logger.info("DB Client Daemon stopped")
    
    def _connect_database(self) -> bool:
        """Connect to MongoDB database"""
        try:
            self.client = pymongo.MongoClient(
                self.mongodb_host,
                self.mongodb_port,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection
            self.client.admin.command('ping')
            self.database = self.client[self.database_name]
            self.connected = True
            
            # Start backup thread if enabled
            if self.backup_enabled:
                self._start_backup()
            
            logger.info(f"Connected to MongoDB database: {self.database_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            self.connected = False
            return False
    
    def _disconnect_database(self):
        """Disconnect from database"""
        if self.client:
            self.client.close()
            self.connected = False
            logger.info("Disconnected from database")
    
    def _start_backup(self):
        """Start automatic backup thread"""
        self.backup_running = True
        self.backup_thread = threading.Thread(target=self._backup_worker)
        self.backup_thread.daemon = True
        self.backup_thread.start()
        logger.info("Automatic backup started")
    
    def _stop_backup(self):
        """Stop automatic backup"""
        self.backup_running = False
        if self.backup_thread:
            self.backup_thread.join(timeout=5)
        logger.info("Automatic backup stopped")
    
    def _backup_worker(self):
        """Worker thread for automatic backups"""
        while self.backup_running:
            try:
                time.sleep(self.backup_interval)
                if self.backup_running:
                    self._perform_backup()
            except Exception as e:
                logger.error(f"Error in backup worker: {e}")
    
    def _perform_backup(self):
        """Perform database backup"""
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"backup_{timestamp}"
            
            # Create backup directory
            backup_dir = Path("backups")
            backup_dir.mkdir(exist_ok=True)
            
            # Export collections
            collections = self.database.list_collection_names()
            for collection_name in collections:
                collection = self.database[collection_name]
                documents = list(collection.find())
                
                backup_file = backup_dir / f"{backup_name}_{collection_name}.json"
                with open(backup_file, 'w') as f:
                    json.dump(documents, f, default=str, indent=2)
            
            logger.info(f"Backup completed: {backup_name}")
            
            # Notify master core
            self.send_to_master_core(
                MessageType.STATUS,
                {"backup_completed": backup_name},
                Priority.NORMAL
            )
            
        except Exception as e:
            logger.error(f"Backup failed: {e}")
    
    def _handle_db_command(self, message: NodeMessage, addr: tuple):
        """Handle database commands"""
        command = message.payload.get("command")
        
        if command == "create_collection":
            self._create_collection(message.payload.get("collection_name"))
        elif command == "drop_collection":
            self._drop_collection(message.payload.get("collection_name"))
        elif command == "get_stats":
            self._get_database_stats()
        elif command == "insert_one":
            collection_name = message.payload.get("collection")
            data = message.payload.get("data")
            if collection_name and data:
                try:
                    # Ensure TTL index exists on collection
                    self._ensure_ttl_index(collection_name)
                    
                    # Add created_at field for TTL (MongoDB standard pattern)
                    # Use UTC time for consistency
                    data["created_at"] = datetime.datetime.utcnow()
                    
                    result = self.database[collection_name].insert_one(data)
                    logger.info(f"Document inserted in {collection_name} with ID: {result.inserted_id}")
                    
                    # Send acknowledgment back to sender
                    response = NodeMessage(
                        message_id=str(uuid.uuid4()),
                        type=MessageType.RESPONSE,
                        priority=Priority.NORMAL,
                        source="db_client",
                        destination=message.source,
                        payload={"status": "success", "inserted_id": str(result.inserted_id)},
                        timestamp=datetime.datetime.now(),
                        requires_ack=False
                    )
                    self._send_message(response, addr)
                except Exception as e:
                    logger.error(f"Error inserting document: {e}")
                    # Send error response
                    response = NodeMessage(
                        message_id=str(uuid.uuid4()),
                        type=MessageType.RESPONSE,
                        priority=Priority.NORMAL,
                        source="db_client",
                        destination=message.source,
                        payload={"status": "error", "message": str(e)},
                        timestamp=datetime.datetime.now(),
                        requires_ack=False
                    )
                    self._send_message(response, addr)
            else:
                logger.error("Missing collection or data parameters for insert_one")
        elif command == "query_data":
            # Delegate to dedicated query handler
            self._handle_query_data(message, addr)
    
    def _handle_query_data(self, message: NodeMessage, addr: tuple):
        """Handle data query requests with FILTER and SORT support
        
        Expected payload:
        {
            "collection": str,
            "query": dict (MongoDB filter),
            "sort": list of tuples [(field, direction)],  # e.g., [("timestamp", -1)]
            "limit": int (default: 100),
            "skip": int (default: 0)
        }
        """
        try:
            # Validate required parameters
            collection_name = message.payload.get("collection")
            if not collection_name:
                raise ValueError("Missing required parameter: 'collection'")
            
            if not self.connected:
                raise ConnectionError("Database not connected")
            
            # Extract query parameters with defaults
            query_filter = message.payload.get("query", {})
            sort_spec = message.payload.get("sort", [])
            limit = message.payload.get("limit", 100)
            skip = message.payload.get("skip", 0)
            
            # Build MongoDB query with extensible pattern
            collection = self.database[collection_name]
            mongo_query = collection.find(query_filter)
            
            # Apply sort if specified (extensible for future features)
            if sort_spec:
                # MongoDB sort accepts list of tuples directly
                # sort_spec: [("field1", 1), ("field2", -1)]
                mongo_query = mongo_query.sort(sort_spec)
            
            # Apply pagination
            if skip > 0:
                mongo_query = mongo_query.skip(skip)
            mongo_query = mongo_query.limit(limit)
            
            # Execute query and convert ObjectId to strings
            results = list(mongo_query)
            self._convert_objectids(results)
            
            # Send success response
            response_payload = {
                "status": "success",
                "collection": collection_name,
                "query_results": results,
                "count": len(results),
                "query_params": {
                    "filter": query_filter,
                    "sort": sort_spec,
                    "limit": limit,
                    "skip": skip
                }
            }
            
            # Echo back request_id if present (for Master Core async callback tracking)
            request_id = message.payload.get("request_id")
            if request_id:
                response_payload["request_id"] = request_id
            
            response = NodeMessage(
                message_id=str(uuid.uuid4()),
                type=MessageType.RESPONSE,
                priority=Priority.NORMAL,
                source=self.node_name,
                destination=message.source,
                payload=response_payload,
                timestamp=time.time(),
                requires_ack=False
            )
            
            # If responding to master_core, use its IPC server address, not the ephemeral port
            if message.source == "master_core":
                # Master Core IPC server is on port 14551 (not the UDP server on 14550)
                master_core_addr = (self.master_core_host, 14551)
                logger.info(f"DEBUG: Sending response to Master Core IPC server at {master_core_addr} (source: {message.source})")
                self._send_message(response, master_core_addr)
                logger.info(f"DEBUG: Response sent to Master Core, request_id: {request_id}")
            else:
                # For other nodes, use the address from the received message
                self._send_message(response, addr)
            logger.info(f"Query executed successfully on {collection_name}: {len(results)} documents")
            
        except ValueError as e:
            # Parameter validation error
            error_payload = {
                "status": "error",
                "error_type": "validation_error",
                "message": str(e),
                "collection": message.payload.get("collection", "unknown")
            }
            
            # Echo back request_id if present
            request_id = message.payload.get("request_id")
            if request_id:
                error_payload["request_id"] = request_id
            
            error_response = NodeMessage(
                message_id=str(uuid.uuid4()),
                type=MessageType.RESPONSE,
                priority=Priority.NORMAL,
                source=self.node_name,
                destination=message.source,
                payload=error_payload,
                timestamp=time.time(),
                requires_ack=False
            )
            
            # If responding to master_core, use its IPC server address
            if message.source == "master_core":
                master_core_addr = (self.master_core_host, 14551)
                self._send_message(error_response, master_core_addr)
            else:
                self._send_message(error_response, addr)
            logger.error(f"Query validation error: {e}")
            
        except ConnectionError as e:
            # Database connection error
            error_payload = {
                "status": "error",
                "error_type": "connection_error",
                "message": str(e),
                "collection": message.payload.get("collection", "unknown")
            }
            
            # Echo back request_id if present
            request_id = message.payload.get("request_id")
            if request_id:
                error_payload["request_id"] = request_id
            
            error_response = NodeMessage(
                message_id=str(uuid.uuid4()),
                type=MessageType.RESPONSE,
                priority=Priority.NORMAL,
                source=self.node_name,
                destination=message.source,
                payload=error_payload,
                timestamp=time.time(),
                requires_ack=False
            )
            
            # If responding to master_core, use its IPC server address
            if message.source == "master_core":
                master_core_addr = (self.master_core_host, 14551)
                self._send_message(error_response, master_core_addr)
            else:
                self._send_message(error_response, addr)
            logger.error(f"Query connection error: {e}")
            
        except Exception as e:
            # Generic database error
            error_payload = {
                "status": "error",
                "error_type": "database_error",
                "message": str(e),
                "collection": message.payload.get("collection", "unknown")
            }
            
            # Echo back request_id if present
            request_id = message.payload.get("request_id")
            if request_id:
                error_payload["request_id"] = request_id
            
            error_response = NodeMessage(
                message_id=str(uuid.uuid4()),
                type=MessageType.RESPONSE,
                priority=Priority.NORMAL,
                source=self.node_name,
                destination=message.source,
                payload=error_payload,
                timestamp=time.time(),
                requires_ack=False
            )
            
            # If responding to master_core, use its IPC server address
            if message.source == "master_core":
                master_core_addr = (self.master_core_host, 14551)
                self._send_message(error_response, master_core_addr)
            else:
                self._send_message(error_response, addr)
            logger.error(f"Query failed: {e}", exc_info=True)
    
    def _convert_objectids(self, documents: List[Dict[str, Any]]):
        """Convert MongoDB ObjectId and datetime to JSON-serializable types (recursive)
        
        This method recursively converts all ObjectId instances to strings and
        datetime objects to ISO format strings to ensure JSON serialization compatibility.
        """
        from bson import ObjectId
        
        for doc in documents:
            if isinstance(doc, dict):
                for key, value in doc.items():
                    if isinstance(value, ObjectId):
                        doc[key] = str(value)
                    elif isinstance(value, datetime.datetime):
                        doc[key] = value.isoformat()
                    elif isinstance(value, dict):
                        self._convert_objectids([value])
                    elif isinstance(value, list):
                        self._convert_objectids(value)
    
    def _handle_backup(self, message: NodeMessage, addr: tuple):
        """Handle backup requests"""
        self._perform_backup()
    
    def _handle_restore(self, message: NodeMessage, addr: tuple):
        """Handle restore requests"""
        backup_name = message.payload.get("backup_name")
        if backup_name:
            self._restore_backup(backup_name)
    
    def _restore_backup(self, backup_name: str):
        """Restore database from backup"""
        try:
            backup_dir = Path("backups")
            
            # Find backup files
            backup_files = list(backup_dir.glob(f"{backup_name}_*.json"))
            
            for backup_file in backup_files:
                collection_name = backup_file.stem.split('_', 1)[1]
                
                with open(backup_file, 'r') as f:
                    documents = json.load(f)
                
                collection = self.database[collection_name]
                collection.insert_many(documents)
            
            logger.info(f"Restore completed: {backup_name}")
            
        except Exception as e:
            logger.error(f"Restore failed: {e}")
    
    def _ensure_ttl_index(self, collection_name: str) -> bool:
        """Ensure TTL index exists on collection's created_at field
        
        Creates a TTL index on the 'created_at' field if it doesn't exist.
        The index will automatically expire documents after data_ttl_days.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            True if index exists or was created successfully, False otherwise
        """
        try:
            if not self.connected:
                return False
            
            collection = self.database[collection_name]
            
            # Convert TTL days to seconds (MongoDB TTL uses seconds)
            expire_after_seconds = self.data_ttl_days * 86400  # days * seconds per day
            
            # Check if TTL index already exists
            indexes = collection.list_indexes()
            ttl_index_exists = False
            for index in indexes:
                if index.get("name") == "created_at_1":
                    # Check if it's a TTL index
                    if "expireAfterSeconds" in index:
                        # Update if TTL value changed
                        if index["expireAfterSeconds"] != expire_after_seconds:
                            logger.info(f"Updating TTL index on {collection_name} from {index['expireAfterSeconds']}s to {expire_after_seconds}s")
                            # Use collMod to update TTL value
                            self.database.command({
                                "collMod": collection_name,
                                "index": {
                                    "keyPattern": {"created_at": 1},
                                    "expireAfterSeconds": expire_after_seconds
                                }
                            })
                        ttl_index_exists = True
                        break
            
            if not ttl_index_exists:
                # Create TTL index on created_at field
                collection.create_index(
                    "created_at",
                    expireAfterSeconds=expire_after_seconds,
                    name="created_at_1"
                )
                logger.info(f"Created TTL index on {collection_name}.created_at (expires after {self.data_ttl_days} days)")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to ensure TTL index on {collection_name}: {e}")
            return False
    
    def _create_collection(self, collection_name: str) -> bool:
        """Create a new collection with TTL index
        
        Args:
            collection_name: Name of the collection to create
            
        Returns:
            True if collection was created successfully, False otherwise
        """
        try:
            if not self.connected:
                logger.error("Database not connected")
                return False
            
            if not collection_name:
                logger.error("Collection name is required")
                return False
            
            # Create collection (MongoDB creates collections lazily, but we can ensure it exists)
            collection = self.database[collection_name]
            
            # Ensure TTL index exists
            self._ensure_ttl_index(collection_name)
            
            logger.info(f"Collection {collection_name} created/verified with TTL index")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create collection {collection_name}: {e}")
            return False
    
    def _drop_collection(self, collection_name: str) -> bool:
        """Drop a collection
        
        Args:
            collection_name: Name of the collection to drop
            
        Returns:
            True if collection was dropped successfully, False otherwise
        """
        try:
            if not self.connected:
                logger.error("Database not connected")
                return False
            
            if not collection_name:
                logger.error("Collection name is required")
                return False
            
            self.database.drop_collection(collection_name)
            logger.info(f"Collection {collection_name} dropped")
            return True
            
        except Exception as e:
            logger.error(f"Failed to drop collection {collection_name}: {e}")
            return False
    
    def _get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics
        
        Returns:
            Dictionary containing database statistics
        """
        try:
            if not self.connected:
                return {"error": "Database not connected"}
            
            stats = {
                "database_name": self.database_name,
                "collections": [],
                "total_collections": 0,
                "total_documents": 0
            }
            
            collections = self.database.list_collection_names()
            stats["total_collections"] = len(collections)
            
            for collection_name in collections:
                collection = self.database[collection_name]
                count = collection.count_documents({})
                stats["total_documents"] += count
                
                # Get index information
                indexes = list(collection.list_indexes())
                ttl_index = None
                for idx in indexes:
                    if "expireAfterSeconds" in idx:
                        ttl_index = {
                            "field": list(idx["key"].keys())[0],
                            "expire_after_seconds": idx["expireAfterSeconds"],
                            "expire_after_days": idx["expireAfterSeconds"] / 86400
                        }
                        break
                
                collection_info = {
                    "name": collection_name,
                    "document_count": count,
                    "ttl_index": ttl_index
                }
                stats["collections"].append(collection_info)
            
            logger.info(f"Database stats retrieved: {stats['total_collections']} collections, {stats['total_documents']} documents")
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {"error": str(e)}
    
    def insert_data(self, collection_name: str, data: Dict[str, Any]) -> bool:
        """Insert data into collection with TTL
        
        Automatically adds created_at field and ensures TTL index exists.
        Documents will expire after data_ttl_days as configured.
        
        Args:
            collection_name: Name of the collection
            data: Dictionary containing document data
            
        Returns:
            True if insertion was successful, False otherwise
        """
        try:
            if not self.connected:
                return False
            
            # Ensure TTL index exists on collection
            self._ensure_ttl_index(collection_name)
            
            # Add created_at field for TTL (MongoDB standard pattern)
            # Use UTC time for consistency
            data["created_at"] = datetime.datetime.utcnow()
            
            collection = self.database[collection_name]
            result = collection.insert_one(data)
            
            logger.debug(f"Data inserted into {collection_name} with ID: {result.inserted_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert data: {e}")
            return False
    
    def query_data(self, collection_name: str, query: Dict[str, Any] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Query data from collection"""
        try:
            if not self.connected:
                return []
            
            collection = self.database[collection_name]
            if query:
                results = list(collection.find(query).limit(limit))
            else:
                results = list(collection.find().limit(limit))
            
            return results
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
    
    def get_database_status(self) -> Dict[str, Any]:
        """Get database-specific status"""
        base_status = self.get_status()
        base_status.update({
            "mongodb_host": self.mongodb_host,
            "mongodb_port": self.mongodb_port,
            "database_name": self.database_name,
            "connected": self.connected,
            "backup_running": self.backup_running,
            "data_ttl_days": self.data_ttl_days
        })
        return base_status

def main():
    """Main entry point for DB Client Node"""
    import argparse
    import sys
    import os
    
    parser = argparse.ArgumentParser(description="DB Client Node")
    parser.add_argument("--config", default="config.json", help="Configuration file")
    parser.add_argument("--daemon", action="store_true", help="Run as daemon")
    
    args = parser.parse_args()
    
    # Load configuration
    config_path = Path(args.config)
    if config_path.exists():
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        # Default configuration
        config = {
            "mongodb_host": "localhost",
            "mongodb_port": 27017,
            "database_name": "OBSDB",
            "node_port": 14552,
            "master_core_host": "localhost",
            "master_core_port": 14550,
            "direct_communication": True,
            "emergency_nodes": ["can_controller"],
            "data_ttl_days": 7,
            "backup_enabled": True,
            "backup_interval": 3600
        }
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start node
    node = DBClientNode(config)
    
    if args.daemon:
        # Run as daemon using simple fork approach
        import os
        import sys
        
        # Fork the process
        try:
            pid = os.fork()
            if pid > 0:
                # Parent process - exit
                sys.exit(0)
        except OSError as e:
            logger.error(f"Failed to fork: {e}")
            sys.exit(1)
        
        # Child process - continue
        os.setsid()  # Create new session
        os.chdir("/")  # Change to root directory
        
        # Write PID file
        with open('/tmp/db_client_node.pid', 'w') as f:
            f.write(str(os.getpid()))
        
        if node.start():
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
        else:
            logger.error("Failed to start DB Client Node")
            sys.exit(1)
    else:
        # Run in foreground
        if node.start():
            node.run_daemon()
        else:
            logger.error("Failed to start DB Client Node")
            sys.exit(1)

if __name__ == "__main__":
    main()
