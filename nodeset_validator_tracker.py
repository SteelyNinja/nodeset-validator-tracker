"""
NodeSet Validator Tracker

Analyzes blockchain transactions to identify NodeSet validators and tracks their
lifecycle including creation, activation, and exits. Provides comprehensive
monitoring and reporting capabilities for NodeSet protocol validator operations.

Requirements:
    - ETH_CLIENT_URL: Ethereum node endpoint
    - BEACON_API_URL: Beacon chain API endpoint (optional, required for exit tracking)

Usage:
    python nodeset_validator_tracker.py
"""

import os
import json
import logging
import requests
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Set, Optional
from web3 import Web3

# Configuration
logging.basicConfig(
    level=logging.INFO,
    filename='nodeset_validator_tracker.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Contract addresses and constants
BEACON_DEPOSIT_CONTRACT = "0x00000000219ab540356cBB839Cbe05303d7705Fa"
NODESET_VAULT_ADDRESS = "0xB266274F55e784689e97b7E363B0666d92e6305B"
MULTICALL_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
BEACON_DEPOSIT_EVENT = "0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5"
DEPLOYMENT_BLOCK = 22318339
CHUNK_SIZE = 5000
CACHE_FILE = "nodeset_validator_tracker_cache.json"

# Beacon chain constants
GENESIS_TIME = 1606824023
SECONDS_PER_SLOT = 12
SLOTS_PER_EPOCH = 32

class NodeSetValidatorTracker:
    """
    Tracks NodeSet validators from blockchain deposits through their complete lifecycle.
    """
    
    def __init__(self, eth_client_url: str, beacon_api_url: Optional[str] = None):
        self.web3 = self._setup_web3(eth_client_url)
        self.beacon_api_url = self._setup_beacon_api(beacon_api_url)
        self.cache = self._load_cache()
    
    def _setup_web3(self, eth_client_url: str) -> Web3:
        """Initialize Web3 connection."""
        web3 = Web3(Web3.HTTPProvider(eth_client_url))
        if not web3.is_connected():
            raise ConnectionError(f"Failed to connect to Ethereum node at {eth_client_url}")
        
        logging.info("Connected to Ethereum node. Latest block: %d", web3.eth.block_number)
        return web3
    
    def _setup_beacon_api(self, beacon_api_url: Optional[str]) -> Optional[str]:
        """Verify beacon API connectivity."""
        if not beacon_api_url:
            return None
        
        try:
            response = requests.get(f"{beacon_api_url}/eth/v1/node/health", timeout=10)
            if response.status_code == 200:
                logging.info("Connected to beacon API at %s", beacon_api_url)
                return beacon_api_url
        except Exception as e:
            logging.warning("Beacon API connection failed: %s", str(e))
        
        return beacon_api_url
    
    def _load_cache(self) -> dict:
        """Load cached analysis state."""
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    logging.info("Loaded cache: processed up to block %d", cache.get('last_block', 0))
                    return cache
            except Exception as e:
                logging.warning("Error loading cache: %s", str(e))

        return {
            'last_block': 0,
            'last_epoch_checked': 0,
            'operator_validators': {},
            'validator_pubkeys': {},
            'validator_indices': {},
            'pending_pubkeys': [],
            'exited_validators': {},
            'exited_pubkeys': [],
            'total_validators': 0,
            'total_exited': 0,
            'processed_transactions': []
        }
    
    def _save_cache(self) -> None:
        """Persist analysis state to disk."""
        try:
            with open(CACHE_FILE, 'w') as f:
                json.dump(self.cache, f, indent=2)
            logging.info("Cache saved: %d active validators, %d exited", 
                        self.cache.get('total_validators', 0), self.cache.get('total_exited', 0))
        except Exception as e:
            logging.error("Error saving cache: %s", str(e))
    
    def _extract_pubkeys_from_deposit(self, tx_receipt: dict) -> List[str]:
        """Extract validator public keys from beacon deposit events."""
        pubkeys = []
        
        for log in tx_receipt['logs']:
            if (log['address'].lower() != BEACON_DEPOSIT_CONTRACT.lower() or
                len(log['topics']) == 0 or
                log['topics'][0].hex().lower() != BEACON_DEPOSIT_EVENT.lower()):
                continue
            
            data = log['data'].hex() if hasattr(log['data'], 'hex') else log['data']
            if data.startswith('0x'):
                data = data[2:]
            
            try:
                from eth_abi import decode
                types = ['bytes', 'bytes', 'bytes', 'bytes', 'bytes']
                decoded = decode(types, bytes.fromhex(data))
                pubkey_bytes = decoded[0]
                
                if len(pubkey_bytes) == 48:
                    pubkey = "0x" + pubkey_bytes.hex()
                    pubkeys.append(pubkey)
                    logging.debug("Extracted pubkey: %s", pubkey[:20])
                    
            except Exception as e:
                logging.debug("Failed to decode deposit event: %s", str(e))
        
        return pubkeys
    
    def _get_validator_index(self, pubkey: str) -> Optional[int]:
        """Retrieve validator index from beacon API."""
        if not self.beacon_api_url:
            return None
        
        try:
            response = requests.get(
                f"{self.beacon_api_url}/eth/v1/beacon/states/head/validators/{pubkey}",
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                return int(data['data']['index'])
        except Exception as e:
            logging.debug("Error getting validator index for %s: %s", pubkey[:20], str(e))
        
        return None
    
    def _get_current_epoch(self) -> int:
        """Get current beacon chain epoch."""
        if not self.beacon_api_url:
            return 0
        
        try:
            response = requests.get(
                f"{self.beacon_api_url}/eth/v1/beacon/headers/head", 
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                slot = int(data['data']['header']['message']['slot'])
                return slot // SLOTS_PER_EPOCH
        except Exception as e:
            logging.debug("Error getting current epoch: %s", str(e))
        
        # Fallback calculation
        import time
        current_time = int(time.time())
        slots_since_genesis = (current_time - GENESIS_TIME) // SECONDS_PER_SLOT
        return slots_since_genesis // SLOTS_PER_EPOCH
    
    def _analyze_transaction(self, tx_hash: str) -> Tuple[Optional[str], int]:
        """Analyze transaction for validator deposits."""
        try:
            tx = self.web3.eth.get_transaction(tx_hash)
            tx_receipt = self.web3.eth.get_transaction_receipt(tx_hash)

            if tx_receipt['status'] != 1:
                return None, 0

            operator = tx['from']
            beacon_deposits = 0
            vault_events = 0

            for log in tx_receipt['logs']:
                if (log['address'].lower() == BEACON_DEPOSIT_CONTRACT.lower() and
                    len(log['topics']) > 0 and
                    log['topics'][0].hex().lower() == BEACON_DEPOSIT_EVENT.lower()):
                    beacon_deposits += 1
                elif log['address'].lower() == NODESET_VAULT_ADDRESS.lower():
                    vault_events += 1

            if beacon_deposits > 0:
                logging.info("TX %s: %d deposits, %d vault events", 
                            tx_hash[:10], beacon_deposits, vault_events)
                return operator, beacon_deposits

            return operator, 0

        except Exception as e:
            logging.debug("Error analyzing transaction %s: %s", tx_hash, str(e))
            return None, 0
    
    def _is_nodeset_transaction(self, tx: dict, tx_receipt: dict) -> bool:
        """Determine if transaction is NodeSet-related."""
        has_vault_logs = any(
            log['address'].lower() == NODESET_VAULT_ADDRESS.lower()
            for log in tx_receipt['logs']
        )
        
        is_multicall = (tx['to'] and 
                       tx['to'].lower() == MULTICALL_ADDRESS.lower())
        
        return has_vault_logs and is_multicall
    
    def _check_validator_exits(self, tracked_indices: Set[int]) -> Dict[int, str]:
        """Check validator exit status via beacon API."""
        if not self.beacon_api_url or not tracked_indices:
            return {}
        
        exited_validators = {}
        batch_size = 50
        validator_list = list(tracked_indices)
        
        for i in range(0, len(validator_list), batch_size):
            batch = validator_list[i:i + batch_size]
            try:
                validator_ids = ','.join(map(str, batch))
                response = requests.get(
                    f"{self.beacon_api_url}/eth/v1/beacon/states/head/validators?id={validator_ids}",
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    for validator in data.get('data', []):
                        index = int(validator['index'])
                        status = validator['status']
                        
                        if status in ['exited_unslashed', 'exited_slashed', 
                                     'withdrawal_possible', 'withdrawal_done']:
                            exited_validators[index] = status
                            logging.info("Validator %d status: %s", index, status)
                
                # Rate limiting
                if i + batch_size < len(validator_list):
                    import time
                    time.sleep(0.1)
                    
            except Exception as e:
                logging.debug("Error checking validator batch: %s", str(e))
                continue
        
        return exited_validators
    
    def scan_validators(self) -> Tuple[dict, int]:
        """Scan blockchain for NodeSet validator deposits."""
        start_block = max(self.cache['last_block'] + 1, DEPLOYMENT_BLOCK)
        current_block = self.web3.eth.block_number

        operator_validators = defaultdict(int, self.cache['operator_validators'])
        total_validators = self.cache['total_validators']
        processed_txs = set(self.cache['processed_transactions'])

        print(f"Scanning blocks {start_block:,} to {current_block:,}")
        if self.cache['last_block'] > 0:
            print(f"Resuming from block {self.cache['last_block']:,}")
            print(f"Previous total: {total_validators} validators")

        blocks_processed = 0
        new_validators = 0

        for chunk_start in range(start_block, current_block + 1, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE - 1, current_block)

            try:
                beacon_filter = {
                    'fromBlock': chunk_start,
                    'toBlock': chunk_end,
                    'address': BEACON_DEPOSIT_CONTRACT,
                    'topics': [BEACON_DEPOSIT_EVENT]
                }

                deposit_events = self.web3.eth.get_logs(beacon_filter)

                if deposit_events:
                    print(f"Blocks {chunk_start:,}-{chunk_end:,}: {len(deposit_events)} deposits")
                    
                    tx_deposits = defaultdict(list)
                    for event in deposit_events:
                        tx_hash = event['transactionHash'].hex()
                        tx_deposits[tx_hash].append(event)

                    for tx_hash, deposits in tx_deposits.items():
                        if tx_hash in processed_txs:
                            continue

                        try:
                            tx = self.web3.eth.get_transaction(tx_hash)
                            tx_receipt = self.web3.eth.get_transaction_receipt(tx_hash)

                            if self._is_nodeset_transaction(tx, tx_receipt):
                                operator, validator_count = self._analyze_transaction(tx_hash)

                                if operator and validator_count > 0:
                                    operator_validators[operator] += validator_count
                                    total_validators += validator_count
                                    new_validators += validator_count
                                    processed_txs.add(tx_hash)

                        except Exception as e:
                            logging.debug("Error processing transaction %s: %s", tx_hash, str(e))
                            continue
                            
                elif chunk_start % 100000 == 0:
                    print(f"Blocks {chunk_start:,}-{chunk_end:,}: No deposits")

                blocks_processed += (chunk_end - chunk_start + 1)

                if blocks_processed % 50000 == 0:
                    self.cache.update({
                        'last_block': chunk_end,
                        'operator_validators': dict(operator_validators),
                        'total_validators': total_validators,
                        'processed_transactions': list(processed_txs)
                    })
                    self._save_cache()
                    print(f"Progress: {total_validators} validators (+{new_validators} new)")

            except Exception as e:
                logging.error("Error processing blocks %d-%d: %s", chunk_start, chunk_end, str(e))
                continue

        self.cache.update({
            'last_block': current_block,
            'operator_validators': dict(operator_validators),
            'total_validators': total_validators,
            'processed_transactions': list(processed_txs)
        })
        self._save_cache()

        print(f"Scan complete: {total_validators} validators (+{new_validators} new)")
        return dict(operator_validators), total_validators
    
    def track_exits(self) -> Tuple[dict, int]:
        """Track validator exits using beacon chain data."""
        if not self.beacon_api_url:
            return self.cache.get('exited_validators', {}), self.cache.get('total_exited', 0)
        
        operator_pubkeys = defaultdict(list, self.cache.get('validator_pubkeys', {}))
        validator_indices = dict(self.cache.get('validator_indices', {}))
        operator_exited = defaultdict(int, self.cache.get('exited_validators', {}))
        total_exited = self.cache.get('total_exited', 0)
        pending_pubkeys = self.cache.get('pending_pubkeys', [])
        
        processed_txs = self.cache.get('processed_transactions', [])
        current_indices = len(validator_indices)
        target_validators = self.cache.get('total_validators', 0)
        
        # Extract pubkeys if needed
        if current_indices < target_validators:
            print(f"Extracting validator data from {len(processed_txs)} transactions")
            print(f"Current: {current_indices} indices, {len(pending_pubkeys)} pending, target: {target_validators}")
            
            for i, tx_hash in enumerate(processed_txs):
                try:
                    tx_receipt = self.web3.eth.get_transaction_receipt(tx_hash)
                    tx = self.web3.eth.get_transaction(tx_hash)
                    operator = tx['from']
                    
                    pubkeys = self._extract_pubkeys_from_deposit(tx_receipt)
                    if pubkeys:
                        for pubkey in pubkeys:
                            if pubkey not in operator_pubkeys[operator]:
                                operator_pubkeys[operator].append(pubkey)
                            
                            if pubkey not in validator_indices and pubkey not in pending_pubkeys:
                                index = self._get_validator_index(pubkey)
                                if index is not None:
                                    validator_indices[pubkey] = index
                                else:
                                    pending_pubkeys.append(pubkey)
                    
                    if (i + 1) % 50 == 0:
                        print(f"Processed {i + 1}/{len(processed_txs)}: {len(validator_indices)} indices, {len(pending_pubkeys)} pending")
                        
                except Exception as e:
                    logging.debug("Error extracting from transaction %s: %s", tx_hash, str(e))
                    continue
            
            print(f"Extraction complete: {len(validator_indices)} indices, {len(pending_pubkeys)} pending")
        
        # Check pending activations
        if pending_pubkeys:
            print(f"Checking {len(pending_pubkeys)} pending validators")
            newly_activated = 0
            still_pending = []
            
            for pubkey in pending_pubkeys:
                index = self._get_validator_index(pubkey)
                if index is not None:
                    validator_indices[pubkey] = index
                    newly_activated += 1
                else:
                    still_pending.append(pubkey)
            
            if newly_activated > 0:
                print(f"Activated: {newly_activated} validators")
            
            pending_pubkeys = still_pending
        
        # Check for exits
        if validator_indices:
            print(f"Checking exit status for {len(validator_indices)} validators")
            tracked_indices = set(validator_indices.values())
            exited_statuses = self._check_validator_exits(tracked_indices)
            
            if exited_statuses:
                print(f"Found {len(exited_statuses)} validators with exit status")
                
                new_exits = 0
                exited_pubkeys = set(self.cache.get('exited_pubkeys', []))
                
                for pubkey, index in validator_indices.items():
                    if index in exited_statuses and pubkey not in exited_pubkeys:
                        for operator, pubkeys in operator_pubkeys.items():
                            if pubkey in pubkeys:
                                status = exited_statuses[index]
                                operator_exited[operator] += 1
                                new_exits += 1
                                exited_pubkeys.add(pubkey)
                                print(f"Exit: Validator {index} ({operator[:10]}...) - {status}")
                                break
                
                self.cache['exited_pubkeys'] = list(exited_pubkeys)
                
                if new_exits == 0:
                    print("All exits previously tracked")
                else:
                    print(f"New exits found: {new_exits}")
                
                total_exited = sum(operator_exited.values())
            else:
                print("No exited validators found")
        
        # Update cache
        self.cache.update({
            'validator_pubkeys': {k: list(v) for k, v in operator_pubkeys.items()},
            'validator_indices': validator_indices,
            'pending_pubkeys': pending_pubkeys,
            'exited_validators': dict(operator_exited),
            'total_exited': total_exited,
            'last_epoch_checked': self._get_current_epoch()
        })
        
        return dict(operator_exited), total_exited
    
    def generate_report(self, operator_validators: dict, operator_exited: dict, 
                       total_validators: int, total_exited: int) -> None:
        """Generate comprehensive validator status report."""
        
        # Calculate active validators per operator
        active_per_operator = {}
        for operator, total_count in operator_validators.items():
            exited_count = operator_exited.get(operator, 0)
            active_count = total_count - exited_count
            if active_count > 0:
                active_per_operator[operator] = active_count
        
        print("\n" + "="*70)
        print("NODESET VALIDATOR ANALYSIS REPORT")
        print("="*70)
        
        # Active validator distribution
        print("\n=== ACTIVE VALIDATOR DISTRIBUTION ===")
        validator_counts = Counter(active_per_operator.values())
        sorted_counts = sorted(validator_counts.keys())
        total_operators = len(active_per_operator)
        total_active = sum(active_per_operator.values())

        for validator_count in sorted_counts:
            operator_count = validator_counts[validator_count]
            print(f"Operators with {validator_count} active validators: {operator_count}")

        print(f"\nActive validators: {total_active}")
        print(f"Exited validators: {total_exited}")
        print(f"Total operators with active validators: {total_operators}")
        print(f"Maximum validators per operator: {sorted_counts[-1] if sorted_counts else 0}")

        if total_active > 0:
            max_exposure = (sorted_counts[-1] / total_active) if sorted_counts else 0
            print(f"Maximum operator concentration: {max_exposure:.4f}")

        # Exit statistics
        if operator_exited and any(operator_exited.values()):
            print("\n=== EXIT STATISTICS ===")
            exit_counts = Counter([c for c in operator_exited.values() if c > 0])
            
            for exit_count in sorted(exit_counts.keys()):
                operator_count = exit_counts[exit_count]
                print(f"Operators with {exit_count} exits: {operator_count}")

        # Top operators
        print("\n=== TOP OPERATORS ===")
        operator_list = [(addr, count) for addr, count in active_per_operator.items()]
        operator_list.sort(key=lambda x: x[1], reverse=True)

        for addr, active_count in operator_list[:5]:
            exited_count = operator_exited.get(addr, 0)
            total_ever = operator_validators.get(addr, 0)
            if exited_count > 0:
                print(f"{active_count} active ({total_ever} total, {exited_count} exited): {addr}")
            else:
                print(f"{active_count} validators: {addr}")
        
        # Fully exited operators
        fully_exited = [(op, cnt) for op, cnt in operator_validators.items() 
                       if operator_exited.get(op, 0) == cnt and cnt > 0]
        
        if fully_exited:
            print("\n=== FULLY EXITED OPERATORS ===")
            for operator, count in fully_exited:
                print(f"All {count} validators exited: {operator}")
    
    def run_analysis(self) -> None:
        """Execute complete validator analysis."""
        try:
            print("NodeSet Validator Tracker")
            print(f"Ethereum node: Connected")
            print(f"Beacon API: {'Connected' if self.beacon_api_url else 'Disabled'}")
            
            # Scan for validators
            operator_validators, total_validators = self.scan_validators()
            
            # Track exits if beacon API available
            operator_exited, total_exited = {}, 0
            if self.beacon_api_url and total_validators > 0:
                print(f"\nTracking exits for {total_validators} validators")
                operator_exited, total_exited = self.track_exits()
                self._save_cache()
            
            # Generate report
            self.generate_report(operator_validators, operator_exited, 
                               total_validators, total_exited)
            
            logging.info("Analysis completed successfully")
            
        except Exception as e:
            logging.error("Analysis failed: %s", str(e))
            raise


def main():
    """Main execution function."""
    eth_client_url = os.getenv('ETH_CLIENT_URL')
    beacon_api_url = os.getenv('BEACON_API_URL')
    
    tracker = NodeSetValidatorTracker(eth_client_url, beacon_api_url)
    tracker.run_analysis()


if __name__ == "__main__":
    main()
