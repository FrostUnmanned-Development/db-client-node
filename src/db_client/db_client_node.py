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
from pathlib import Path
from typing import Dict, Any, Optional, List
import json
import os
import sys

# Add parent directory to path for base_node import
sys.path.append(str(Path(__file__).parent.parent.parent / "src" / "onboard_core"))

from base_node import BaseNode, MessageType, Priority

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
        self.register_handler("db_command", self._handle_db_command)
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
    
    def _handle_query_data(self, message: NodeMessage, addr: tuple):
        """Handle data query requests"""
        collection_name = message.payload.get("collection")
        query = message.payload.get("query", {})
        limit = message.payload.get("limit", 100)
        
        try:
            collection = self.database[collection_name]
            results = list(collection.find(query).limit(limit))
            
            # Send results back
            response = NodeMessage(
                id=str(uuid.uuid4()),
                type=MessageType.RESPONSE,
                priority=Priority.NORMAL,
                source=self.node_name,
                destination=message.source,
                payload={
                    "query_results": results,
                    "count": len(results)
                },
                timestamp=time.time()
            )
            self._send_message(response, addr)
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
    
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
    
    def insert_data(self, collection_name: str, data: Dict[str, Any]) -> bool:
        """Insert data into collection with TTL"""
        try:
            if not self.connected:
                return False
            
            # Add TTL expiration
            expiration_date = datetime.datetime.now() + datetime.timedelta(days=self.data_ttl_days)
            data["ttl_expiration"] = expiration_date
            
            collection = self.database[collection_name]
            result = collection.insert_one(data)
            
            logger.debug(f"Data inserted into {collection_name}: {result.inserted_id}")
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
        # Run as daemon
        import daemon
        from daemon.pidfile import PIDLockFile
        
        pidfile = PIDLockFile('/tmp/db_client_node.pid')
        
        with daemon.DaemonContext(pidfile=pidfile):
            if node.start():
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
            node.stop()
    else:
        # Run in foreground
        if node.start():
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
        node.stop()

if __name__ == "__main__":
    main()
