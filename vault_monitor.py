"""
Standalone Vault Monitor for NodeSet Dashboard

Monitors vault contract events (deposits, withdrawals, validator registrations)
and stores data in JSON format for dashboard consumption.
"""

import os
import json
import time
import logging
import requests
from collections import defaultdict
from typing import Dict, List, Optional, Any
from web3 import Web3
from web3.contract import Contract
from web3._utils.filters import LogFilter
import datetime

# Configuration
logging.basicConfig(
    level=logging.INFO,
    filename='vault_monitor.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
VAULT_ADDRESS = "0xB266274F55e784689e97b7E363B0666d92e6305B"
DEPLOYMENT_BLOCK = 22318339
CHUNK_SIZE = 5000
CACHE_FILE = "vault_events_cache.json"
OUTPUT_FILE = "vault_events.json"

# Vault ABI (minimal - only events we need)
VAULT_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "caller", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "receiver", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "assets", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "shares", "type": "uint256"},
            {"indexed": False, "internalType": "address", "name": "referrer", "type": "address"}
        ],
        "name": "Deposited",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "owner", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "receiver", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "positionTicket", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "shares", "type": "uint256"}
        ],
        "name": "ExitQueueEntered",
        "type": "event"
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": False, "internalType": "bytes", "name": "publicKey", "type": "bytes"}
        ],
        "name": "ValidatorRegistered",
        "type": "event"
    },
    {
        "inputs": [{"internalType": "uint256", "name": "shares", "type": "uint256"}],
        "name": "convertToAssets",
        "outputs": [{"internalType": "uint256", "name": "assets", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]

class VaultMonitor:
    """Standalone vault event monitor with incremental scanning."""
    
    def __init__(self):
        self.web3 = self._setup_web3()
        self.vault_contract = self._setup_contract()
        self.cache = self._load_cache()
        
    def _setup_web3(self) -> Web3:
        """Initialize Web3 connection using environment variables."""
        eth_client_url = os.getenv('ETH_CLIENT_URL')
        if not eth_client_url:
            raise ValueError("ETH_CLIENT_URL environment variable not set")
            
        web3 = Web3(Web3.HTTPProvider(eth_client_url))
        if not web3.is_connected():
            raise ConnectionError(f"Failed to connect to Ethereum node at {eth_client_url}")
            
        logging.info("Connected to Ethereum node. Latest block: %d", web3.eth.block_number)
        return web3
        
    def _setup_contract(self) -> Contract:
        """Initialize vault contract."""
        return self.web3.eth.contract(
            address=Web3.to_checksum_address(VAULT_ADDRESS),
            abi=VAULT_ABI
        )
        
    def _load_cache(self) -> Dict[str, Any]:
        """Load cache from file or create new cache."""
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    logging.info("Loaded cache from %s. Last scanned block: %d", 
                               CACHE_FILE, cache.get('last_scanned_block', DEPLOYMENT_BLOCK))
                    return cache
            except Exception as e:
                logging.warning("Error loading cache: %s", str(e))
                
        # Create new cache
        cache = {
            'last_scanned_block': DEPLOYMENT_BLOCK - 1,
            'events': [],
            'last_update': None
        }
        logging.info("Created new cache starting from block %d", DEPLOYMENT_BLOCK)
        return cache
        
    def _save_cache(self):
        """Save cache to file."""
        try:
            with open(CACHE_FILE, 'w') as f:
                json.dump(self.cache, f, indent=2)
            logging.info("Cache saved to %s", CACHE_FILE)
        except Exception as e:
            logging.error("Error saving cache: %s", str(e))
            
    def _save_output(self):
        """Save events data for dashboard consumption."""
        try:
            output_data = {
                'vault_address': VAULT_ADDRESS,
                'last_update': datetime.datetime.now().isoformat(),
                'total_events': len(self.cache['events']),
                'events': self.cache['events']
            }
            
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(output_data, f, indent=2)
            logging.info("Output data saved to %s with %d events", 
                        OUTPUT_FILE, len(self.cache['events']))
        except Exception as e:
            logging.error("Error saving output: %s", str(e))
            
    def _get_block_timestamp(self, block_number: int) -> int:
        """Get timestamp for a block number."""
        try:
            block = self.web3.eth.get_block(block_number)
            return block['timestamp']
        except Exception as e:
            logging.warning("Error getting block %d timestamp: %s", block_number, str(e))
            return int(time.time())
            
    def _format_deposit_event(self, event_log, block_timestamp: int) -> Dict[str, Any]:
        """Format a deposit event for storage."""
        return {
            'type': 'deposit',
            'block_number': event_log['blockNumber'],
            'transaction_hash': event_log['transactionHash'].hex(),
            'transaction_index': event_log['transactionIndex'],
            'timestamp': block_timestamp,
            'caller': event_log['args']['caller'],
            'receiver': event_log['args']['receiver'],
            'assets': str(event_log['args']['assets']),
            'assets_eth': float(self.web3.from_wei(event_log['args']['assets'], 'ether')),
            'shares': str(event_log['args']['shares']),
            'referrer': event_log['args']['referrer']
        }
        
    def _format_exit_event(self, event_log, block_timestamp: int) -> Dict[str, Any]:
        """Format an exit queue event for storage."""
        # Convert shares to assets at the time of the event
        try:
            assets = self.vault_contract.functions.convertToAssets(
                event_log['args']['shares']
            ).call(block_identifier=event_log['blockNumber'])
            assets_eth = float(self.web3.from_wei(assets, 'ether'))
        except Exception as e:
            logging.warning("Error converting shares to assets: %s", str(e))
            assets = event_log['args']['shares']  # Fallback
            assets_eth = 0.0
            
        return {
            'type': 'withdrawal',
            'block_number': event_log['blockNumber'],
            'transaction_hash': event_log['transactionHash'].hex(),
            'transaction_index': event_log['transactionIndex'],
            'timestamp': block_timestamp,
            'owner': event_log['args']['owner'],
            'receiver': event_log['args']['receiver'],
            'position_ticket': str(event_log['args']['positionTicket']),
            'shares': str(event_log['args']['shares']),
            'estimated_assets': str(assets),
            'estimated_assets_eth': assets_eth
        }
        
    def _format_validator_event(self, event_log, block_timestamp: int) -> Dict[str, Any]:
        """Format a validator registration event for storage."""
        public_key = '0x' + event_log['args']['publicKey'].hex()
        
        return {
            'type': 'validator_registration',
            'block_number': event_log['blockNumber'],
            'transaction_hash': event_log['transactionHash'].hex(),
            'transaction_index': event_log['transactionIndex'],
            'timestamp': block_timestamp,
            'public_key': public_key
        }
        
    def _scan_block_range(self, from_block: int, to_block: int) -> List[Dict[str, Any]]:
        """Scan a block range for vault events."""
        events = []
        
        try:
            # Get deposit events
            deposit_filter = self.vault_contract.events.Deposited.create_filter(
                fromBlock=from_block,
                toBlock=to_block
            )
            
            for event_log in deposit_filter.get_all_entries():
                block_timestamp = self._get_block_timestamp(event_log['blockNumber'])
                events.append(self._format_deposit_event(event_log, block_timestamp))
                
            # Get exit queue events
            exit_filter = self.vault_contract.events.ExitQueueEntered.create_filter(
                fromBlock=from_block,
                toBlock=to_block
            )
            
            for event_log in exit_filter.get_all_entries():
                block_timestamp = self._get_block_timestamp(event_log['blockNumber'])
                events.append(self._format_exit_event(event_log, block_timestamp))
                
            # Get validator registration events
            validator_filter = self.vault_contract.events.ValidatorRegistered.create_filter(
                fromBlock=from_block,
                toBlock=to_block
            )
            
            for event_log in validator_filter.get_all_entries():
                block_timestamp = self._get_block_timestamp(event_log['blockNumber'])
                events.append(self._format_validator_event(event_log, block_timestamp))
                
        except Exception as e:
            logging.error("Error scanning blocks %d-%d: %s", from_block, to_block, str(e))
            
        return events
        
    def scan_new_events(self):
        """Scan for new events since last update."""
        current_block = self.web3.eth.block_number
        last_scanned = self.cache['last_scanned_block']
        
        if current_block <= last_scanned:
            logging.info("No new blocks to scan")
            return
            
        logging.info("Scanning blocks %d to %d", last_scanned + 1, current_block)
        
        # Scan in chunks
        start_block = last_scanned + 1
        
        while start_block <= current_block:
            end_block = min(start_block + CHUNK_SIZE - 1, current_block)
            
            logging.info("Scanning chunk: blocks %d to %d", start_block, end_block)
            
            new_events = self._scan_block_range(start_block, end_block)
            
            if new_events:
                # Sort events by block number and transaction index
                new_events.sort(key=lambda x: (x['block_number'], x['transaction_index']))
                self.cache['events'].extend(new_events)
                logging.info("Found %d new events in blocks %d-%d", 
                           len(new_events), start_block, end_block)
                
            # Update last scanned block
            self.cache['last_scanned_block'] = end_block
            
            start_block = end_block + 1
            
        # Update cache timestamp
        self.cache['last_update'] = datetime.datetime.now().isoformat()
        
        # Save cache and output
        self._save_cache()
        self._save_output()
        
        logging.info("Scan complete. Total events: %d", len(self.cache['events']))
        
    def get_statistics(self) -> Dict[str, Any]:
        """Generate statistics from cached events."""
        stats = {
            'total_events': len(self.cache['events']),
            'deposits': 0,
            'withdrawals': 0,
            'validator_registrations': 0,
            'total_deposited_eth': 0.0,
            'total_withdrawn_eth': 0.0,
            'unique_depositors': set(),
            'unique_withdrawers': set()
        }
        
        for event in self.cache['events']:
            if event['type'] == 'deposit':
                stats['deposits'] += 1
                stats['total_deposited_eth'] += event['assets_eth']
                stats['unique_depositors'].add(event['caller'])
            elif event['type'] == 'withdrawal':
                stats['withdrawals'] += 1
                stats['total_withdrawn_eth'] += event['estimated_assets_eth']
                stats['unique_withdrawers'].add(event['owner'])
            elif event['type'] == 'validator_registration':
                stats['validator_registrations'] += 1
                
        # Convert sets to counts
        stats['unique_depositors'] = len(stats['unique_depositors'])
        stats['unique_withdrawers'] = len(stats['unique_withdrawers'])
        
        return stats

def main():
    """Main execution function."""
    try:
        # Check environment variables
        if not os.getenv('ETH_CLIENT_URL'):
            raise ValueError("ETH_CLIENT_URL environment variable not set")
            
        monitor = VaultMonitor()
        
        # Scan for new events
        monitor.scan_new_events()
        
        # Generate and log statistics
        stats = monitor.get_statistics()
        logging.info("Statistics: %s", json.dumps(stats, indent=2))
        
        print(f"Vault monitoring complete. Found {stats['total_events']} total events.")
        print(f"Deposits: {stats['deposits']}, Withdrawals: {stats['withdrawals']}, Validator Registrations: {stats['validator_registrations']}")
        print(f"Output saved to {OUTPUT_FILE}")
        
    except Exception as e:
        logging.error("Error in main execution: %s", str(e))
        print(f"Error: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())