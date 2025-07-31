"""
NodeSet Validator Tracker with ENS Support and Manual Override

"""

import os
import json
import time
import logging
import requests
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Set, Optional
from web3 import Web3
import http.client
from eth_abi import decode
import datetime

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
MANUAL_ENS_FILE = "manual_ens_names.json"
AGGREGATE_SIGNATURE = "0x252dba42"

# Beacon chain constants
GENESIS_TIME = 1606824023
SECONDS_PER_SLOT = 12
SLOTS_PER_EPOCH = 32

# ENS constants
ENS_UPDATE_INTERVAL = 3600  # 1 hour in seconds

class NodeSetValidatorTracker:
    """
    Tracks NodeSet validators from blockchain deposits through their complete lifecycle.
    Now includes ENS name resolution, caching, and manual ENS override support.
    """

    def __init__(self, eth_client_url: str, beacon_api_url: Optional[str] = None, etherscan_api_key: Optional[str] = None, 
                 clickhouse_host: str = "192.168.202.250", clickhouse_port: int = 8123):
        self.web3 = self._setup_web3(eth_client_url)
        self.beacon_api_url = self._setup_beacon_api(beacon_api_url)
        self.etherscan_api_key = etherscan_api_key
        self.manual_ens_names = self._load_manual_ens_names()
        self.cache = self._load_cache()
        
        # ClickHouse configuration
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_url = f"http://{clickhouse_host}:{clickhouse_port}/"

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

    def _load_manual_ens_names(self) -> Dict[str, str]:
        """Load manual ENS name mappings from JSON file."""
        if os.path.exists(MANUAL_ENS_FILE):
            try:
                with open(MANUAL_ENS_FILE, 'r') as f:
                    manual_names = json.load(f)
                    
                normalized_names = {}
                for address, name in manual_names.items():
                    normalized_address = address.lower()
                    normalized_names[normalized_address] = name
                    
                logging.info("Loaded %d manual ENS name mappings from %s", 
                           len(normalized_names), MANUAL_ENS_FILE)
                return normalized_names
            except Exception as e:
                logging.warning("Error loading manual ENS names from %s: %s", MANUAL_ENS_FILE, str(e))
                return {}
        else:
            self._create_example_manual_ens_file()
            logging.info("Created example manual ENS file: %s", MANUAL_ENS_FILE)
            return {}

    def _create_example_manual_ens_file(self) -> None:
        """Create an example manual ENS names file."""
        example_data = {
            "0x57e67C5C943c3444f7D9bC3b427c619398BFd45a": "vapor.farm",
            "0x1234567890123456789012345678901234567890": "example.operator"
        }
        
        try:
            with open(MANUAL_ENS_FILE, 'w') as f:
                json.dump(example_data, f, indent=2)
            print(f"Created example manual ENS file: {MANUAL_ENS_FILE}")
            print("Add your custom address-to-name mappings to this file.")
        except Exception as e:
            logging.error("Error creating example manual ENS file: %s", str(e))

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
            'processed_transactions': [],
            'operator_performance': {},
            'performance_last_updated': 0,
            'operator_transactions': {},
            'operator_costs': {},
            'cost_last_updated': 0,
            # ENS-related cache entries
            'ens_names': {},
            'ens_sources': {},  # Track source of ENS names (manual/on-chain)
            'ens_last_updated': 0,
            'ens_update_failures': {}
        }

    def _save_cache(self) -> None:
        """Persist analysis state to disk."""
        try:
            with open(CACHE_FILE, 'w') as f:
                json.dump(self.cache, f, indent=2)
            logging.info("Cache saved: %d active validators, %d exited, %d ENS names",
                        self.cache.get('total_validators', 0),
                        self.cache.get('total_exited', 0),
                        len(self.cache.get('ens_names', {})))
        except Exception as e:
            logging.error("Error saving cache: %s", str(e))

    def _resolve_ens_name(self, address: str) -> tuple[Optional[str], Optional[str]]:
        """Resolve ENS name for an Ethereum address, checking manual override first.
        Returns (name, source) where source is 'manual' or 'on-chain'."""
        normalized_address = address.lower()
        if normalized_address in self.manual_ens_names:
            manual_name = self.manual_ens_names[normalized_address]
            logging.info("Using manual ENS name: %s -> %s", address[:10], manual_name)
            return manual_name, 'manual'

        try:
            checksum_address = self.web3.to_checksum_address(address.lower())
            ens_name = self.web3.ens.name(checksum_address)

            if ens_name:
                logging.info("Resolved on-chain ENS: %s -> %s", address[:10], ens_name)
                return ens_name, 'on-chain'
            else:
                logging.debug("No on-chain ENS name found for %s", address[:10])
                return None, None

        except Exception as e:
            logging.debug("On-chain ENS resolution failed for %s: %s", address[:10], str(e))
            return None, None

    def _update_ens_names(self) -> None:
        """Update ENS names for all operator addresses."""
        current_time = int(time.time())
        last_ens_update = self.cache.get('ens_last_updated', 0)

        operator_validators = self.cache.get('operator_validators', {})
        ens_names = self.cache.get('ens_names', {})
        ens_sources = self.cache.get('ens_sources', {})
        ens_failures = self.cache.get('ens_update_failures', {})

        operator_addresses = list(operator_validators.keys())

        if not operator_addresses:
            logging.info("No operator addresses to resolve ENS names for")
            return

        print(f"Updating ENS names for {len(operator_addresses)} operators...")
        print(f"Manual ENS mappings available: {len(self.manual_ens_names)}")
        logging.info("Starting ENS resolution for %d operator addresses (%d manual mappings loaded)", 
                    len(operator_addresses), len(self.manual_ens_names))

        updated_count = 0
        manual_count = 0
        onchain_count = 0
        failed_count = 0

        for i, address in enumerate(operator_addresses):
            try:
                normalized_address = address.lower()
                print(f"Resolving ENS for operator {i+1}/{len(operator_addresses)}: {address[:10]}...")

                ens_name, source = self._resolve_ens_name(address)

                if ens_name and source:
                    ens_names[address] = ens_name
                    ens_sources[address] = source
                    updated_count += 1
                    
                    if source == 'manual':
                        manual_count += 1
                    else:
                        onchain_count += 1
                    
                    if address in ens_failures:
                        del ens_failures[address]
                else:
                    if normalized_address not in self.manual_ens_names:
                        ens_failures[address] = current_time
                    failed_count += 1

                if normalized_address not in self.manual_ens_names:
                    time.sleep(0.1)

                # Progress update every 10 addresses
                if (i + 1) % 10 == 0:
                    print(f"Progress: {i+1}/{len(operator_addresses)} ({updated_count} found, {manual_count} manual, {onchain_count} on-chain)")

            except Exception as e:
                logging.error("Error resolving ENS for %s: %s", address[:10], str(e))
                normalized_address = address.lower()
                if normalized_address not in self.manual_ens_names:
                    ens_failures[address] = current_time
                failed_count += 1
                continue

        # Update cache
        self.cache.update({
            'ens_names': ens_names,
            'ens_sources': ens_sources,
            'ens_last_updated': current_time,
            'ens_update_failures': ens_failures
        })

        print(f"ENS update complete: {updated_count} names found ({manual_count} manual, {onchain_count} on-chain), {failed_count} failed")
        logging.info("ENS update completed: %d names resolved (%d manual, %d on-chain), %d failed, %d total cached",
                    updated_count, manual_count, onchain_count, failed_count, len(ens_names))

    def get_ens_name(self, address: str) -> Optional[str]:
        """Get ENS name for an address from cache."""
        ens_names = self.cache.get('ens_names', {})
        return ens_names.get(address)

    def format_operator_display(self, address: str) -> str:
        """Format operator address with ENS name if available."""
        ens_name = self.get_ens_name(address)
        if ens_name:
            return f"{ens_name} ({address[:8]}...{address[-6:]})"
        else:
            return f"{address[:8]}...{address[-6:]}"

    def _extract_pubkeys_from_deposit(self, tx_receipt: dict) -> List[str]:
        """Extract validator public keys from beacon deposit events."""
        pubkeys = []

        for log in tx_receipt['logs']:
            if (log['address'].lower() != BEACON_DEPOSIT_CONTRACT.lower() or
                len(log['topics']) == 0):
                continue

            topic_hex = log['topics'][0].hex()
            if not topic_hex.startswith('0x'):
                topic_hex = '0x' + topic_hex

            if topic_hex.lower() != BEACON_DEPOSIT_EVENT.lower():
                continue

            data = log['data'].hex() if hasattr(log['data'], 'hex') else log['data']
            if data.startswith('0x'):
                data = data[2:]

            try:
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
        validator_indices = self.cache.get('validator_indices', {})
        if pubkey in validator_indices:
            cached_index = validator_indices[pubkey]
            if cached_index is not None:
                return cached_index

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
        current_time = int(time.time())
        slots_since_genesis = (current_time - GENESIS_TIME) // SECONDS_PER_SLOT
        return slots_since_genesis // SLOTS_PER_EPOCH

    def _epoch_to_timestamp(self, epoch: int) -> int:
        """Convert epoch number to Unix timestamp."""
        if epoch is None or epoch == '18446744073709551615':  # Max uint64 used for "never"
            return None
        
        try:
            epoch_int = int(epoch)
            # Calculate slot number from epoch
            slot = epoch_int * SLOTS_PER_EPOCH
            # Calculate timestamp from slot
            timestamp = GENESIS_TIME + (slot * SECONDS_PER_SLOT)
            return timestamp
        except (ValueError, TypeError):
            return None

    def _analyze_transaction(self, tx_receipt: dict) -> Tuple[Optional[str], int]:
        """Analyze transaction for validator deposits."""
        try:
            if tx_receipt['status'] != 1:
                return None, 0

            operator = tx_receipt['from']
            beacon_deposits = 0
            vault_events = 0

            for log in tx_receipt['logs']:
                if log['address'].lower() == BEACON_DEPOSIT_CONTRACT.lower() and len(log['topics']) > 0:
                    topic_hex = log['topics'][0].hex()
                    if not topic_hex.startswith('0x'):
                        topic_hex = '0x' + topic_hex

                    if topic_hex.lower() == BEACON_DEPOSIT_EVENT.lower():
                        beacon_deposits += 1
                elif log['address'].lower() == NODESET_VAULT_ADDRESS.lower():
                    vault_events += 1

            if beacon_deposits > 0:
                logging.info("TX %s: %d deposits, %d vault events",
                            tx_receipt['transactionHash'][:10].hex(), beacon_deposits, vault_events)
                return operator, beacon_deposits

            return operator, 0

        except Exception as e:
            logging.debug("Error analyzing transaction %s: %s",
                         tx_receipt.get('transactionHash', 'unknown'), str(e))
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

    def _check_validator_exits(self, tracked_indices: Set[int]) -> Dict[int, dict]:
        """Check validator exit status via ClickHouse database with detailed exit information."""
        if not tracked_indices:
            return {}

        exited_validators = {}
        indices_list = list(tracked_indices)
        
        # Query ClickHouse for validator statuses
        indices_str = ','.join(map(str, indices_list))
        query = f"""
        SELECT 
            val_id,
            val_status,
            epoch,
            val_balance,
            val_effective_balance,
            val_slashed
        FROM default.validators_summary 
        WHERE val_id IN ({indices_str})
            AND epoch = (SELECT MAX(epoch) FROM default.validators_summary)
            AND val_status IN ('exited_unslashed', 'exited_slashed', 
                              'withdrawal_possible', 'withdrawal_done')
        """
        
        try:
            logging.info("Querying ClickHouse for validator exit statuses")
            response = requests.post(
                self.clickhouse_url, 
                data=query,
                timeout=30
            )
            
            if response.status_code == 200:
                lines = response.text.strip().split('\n')
                if lines and lines[0]:  # Check if we have results
                    for line in lines:
                        parts = line.split('\t')
                        if len(parts) >= 6:
                            val_id = int(parts[0])
                            status = parts[1]
                            epoch = int(parts[2])
                            balance = int(parts[3]) if parts[3] != '\\N' else None
                            effective_balance = int(parts[4]) if parts[4] != '\\N' else None
                            slashed = bool(int(parts[5])) if parts[5] != '\\N' else False
                            
                            # For ClickHouse data, we don't have separate exit/withdrawable epochs
                            # We'll use the current epoch as a placeholder for exit epoch
                            exit_info = {
                                'status': status,
                                'exit_epoch': epoch,  # Current epoch from data
                                'withdrawable_epoch': None,  # Not available in ClickHouse
                                'exit_timestamp': self._epoch_to_timestamp(epoch),
                                'withdrawable_timestamp': None,
                                'balance': balance,
                                'effective_balance': effective_balance,
                                'slashed': slashed
                            }
                            
                            exited_validators[val_id] = exit_info
                            logging.info("Validator %d exit details from ClickHouse: status=%s, epoch=%s", 
                                       val_id, status, epoch)
                
                logging.info("Found %d exited validators from ClickHouse", len(exited_validators))
            else:
                logging.error("ClickHouse query failed with status %d", response.status_code)
                
        except Exception as e:
            logging.error("Error querying ClickHouse for validator exits: %s", str(e))
            return {}

        return exited_validators

    def _count_beacon_deposits(self, tx_receipt: dict) -> int:
        """Count the number of beacon deposit events in a transaction."""
        deposit_count = 0

        for log in tx_receipt['logs']:
            if (log['address'].lower() != BEACON_DEPOSIT_CONTRACT.lower() or
                len(log['topics']) == 0):
                continue

            topic_hex = log['topics'][0].hex()
            if not topic_hex.startswith('0x'):
                topic_hex = '0x' + topic_hex

            if topic_hex.lower() == BEACON_DEPOSIT_EVENT.lower():
                deposit_count += 1

        return deposit_count

    def _fetch_operator_transactions(self, operator_address: str) -> List[dict]:
        """Fetch all aggregate transactions for a single operator."""
        if not self.etherscan_api_key:
            return []

        all_transactions = []
        page = 1
        offset = 1000

        while True:
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': operator_address,
                'startblock': DEPLOYMENT_BLOCK,
                'endblock': 99999999,
                'page': page,
                'offset': offset,
                'sort': 'desc',
                'apikey': self.etherscan_api_key
            }

            try:
                response = requests.get('https://api.etherscan.io/api', params=params, timeout=30)
                if response.status_code != 200:
                    break

                data = response.json()
                if data.get('status') == '0':
                    break

                transactions = data.get('result', [])
                if not transactions:
                    break

                aggregate_txs = []
                for tx in transactions:
                    input_data = tx.get('input', '')
                    if (input_data and
                        len(input_data) >= 10 and
                        input_data[:10].lower() == AGGREGATE_SIGNATURE.lower()):

                        tx_timestamp = int(tx['timeStamp'])
                        dt = datetime.datetime.fromtimestamp(tx_timestamp)

                        gas_used = int(tx['gasUsed'])
                        gas_price = int(tx['gasPrice'])
                        txn_fee_wei = gas_used * gas_price
                        total_cost = float(self.web3.from_wei(txn_fee_wei, 'ether'))

                        is_error = int(tx.get('isError', '0'))
                        status = "Failed" if is_error == 1 else "Successful"

                        # Count validators for successful transactions
                        validator_count = 0
                        if status == "Successful":
                            try:
                                tx_receipt = self.web3.eth.get_transaction_receipt(tx['hash'])
                                validator_count = self._count_beacon_deposits(tx_receipt)
                            except Exception as e:
                                logging.debug("Error counting deposits for tx %s: %s", tx['hash'], str(e))
                                validator_count = 0

                        aggregate_txs.append({
                            'hash': tx['hash'],
                            'date': dt.strftime("%Y-%m-%d"),
                            'time': dt.strftime("%H:%M:%S"),
                            'gas_used': gas_used,
                            'gas_price': gas_price,
                            'total_cost_eth': total_cost,
                            'status': status,
                            'validator_count': validator_count
                        })

                all_transactions.extend(aggregate_txs)

                if len(transactions) < offset:
                    break

                page += 1
                time.sleep(0.2)

            except Exception as e:
                logging.debug("Error fetching transactions for %s page %d: %s", operator_address[:10], page, str(e))
                break

        return all_transactions

    def analyze_operator_costs(self) -> None:
        """Analyze transaction costs for all operators."""
        if not self.etherscan_api_key:
            logging.info("Etherscan API key not provided, skipping cost analysis")
            return

        operator_validators = self.cache.get('operator_validators', {})
        operator_transactions = self.cache.get('operator_transactions', {})
        operator_costs = self.cache.get('operator_costs', {})

        last_cost_update = self.cache.get('cost_last_updated', 0)
        current_time = int(time.time())

        if current_time - last_cost_update < 3600:
            logging.info("Cost data updated recently, skipping")
            return

        print(f"Analyzing transaction costs for {len(operator_validators)} operators")

        for i, operator in enumerate(operator_validators.keys()):
            display_name = self.format_operator_display(operator)
            print(f"Fetching costs for operator {i+1}/{len(operator_validators)}: {display_name}...")

            transactions = self._fetch_operator_transactions(operator)

            if transactions:
                operator_transactions[operator] = transactions

                total_cost = sum(tx['total_cost_eth'] for tx in transactions)
                successful_txs = len([tx for tx in transactions if tx['status'] == 'Successful'])
                failed_txs = len([tx for tx in transactions if tx['status'] == 'Failed'])
                avg_cost = total_cost / len(transactions) if transactions else 0
                total_validators_created = sum(tx['validator_count'] for tx in transactions if tx['status'] == 'Successful')

                operator_costs[operator] = {
                    'total_cost_eth': total_cost,
                    'successful_txs': successful_txs,
                    'failed_txs': failed_txs,
                    'avg_cost_per_tx': avg_cost,
                    'total_txs': len(transactions),
                    'total_validators_created': total_validators_created
                }

                logging.info("Operator %s: %d transactions, %.6f ETH total cost, %d validators",
                           display_name, len(transactions), total_cost, total_validators_created)

            time.sleep(0.3)

        self.cache.update({
            'operator_transactions': operator_transactions,
            'operator_costs': operator_costs,
            'cost_last_updated': current_time
        })

        total_operators_with_costs = len([c for c in operator_costs.values() if c['total_txs'] > 0])
        total_cost_all = sum(c['total_cost_eth'] for c in operator_costs.values())
        total_validators_all = sum(c['total_validators_created'] for c in operator_costs.values())
        print(f"Cost analysis complete: {total_operators_with_costs} operators, {total_cost_all:.6f} ETH total, {total_validators_all} validators")

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
                                operator, validator_count = self._analyze_transaction(tx_receipt)

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
        operator_exited = defaultdict(int, self.cache.get('operator_exited', {}))
        total_exited = self.cache.get('total_exited', 0)
        pending_pubkeys = self.cache.get('pending_pubkeys', [])

        processed_txs = self.cache.get('processed_transactions', [])
        current_indices = len(validator_indices)
        target_validators = self.cache.get('total_validators', 0)

        if current_indices < target_validators:
            print(f"Extracting validator data from {len(processed_txs)} transactions")
            print(f"Current: {current_indices} indices, {len(pending_pubkeys)} pending, target: {target_validators}")
            logging.info("Starting validator data extraction: %d transactions to process, %d current indices, %d pending, target: %d",
                        len(processed_txs), current_indices, len(pending_pubkeys), target_validators)

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
                                    logging.info("Mapped validator pubkey %s to index %d", pubkey[:20], index)
                                else:
                                    pending_pubkeys.append(pubkey)
                                    logging.info("Validator pubkey %s not yet activated, added to pending list", pubkey[:20])

                    if (i + 1) % 50 == 0:
                        print(f"Processed {i + 1}/{len(processed_txs)}: {len(validator_indices)} indices, {len(pending_pubkeys)} pending")
                        logging.info("Extraction progress: %d/%d transactions processed, %d indices mapped, %d pending",
                                    i + 1, len(processed_txs), len(validator_indices), len(pending_pubkeys))

                except Exception as e:
                    logging.debug("Error extracting from transaction %s: %s", tx_hash, str(e))
                    continue

            print(f"Extraction complete: {len(validator_indices)} indices, {len(pending_pubkeys)} pending")
            logging.info("Validator extraction completed: %d total indices mapped, %d pending activation",
                        len(validator_indices), len(pending_pubkeys))

        # Check pending activations
        if pending_pubkeys:
            print(f"Checking {len(pending_pubkeys)} pending validators")
            logging.info("Checking activation status for %d pending validators", len(pending_pubkeys))
            newly_activated = 0
            still_pending = []

            for pubkey in pending_pubkeys:
                index = self._get_validator_index(pubkey)
                if index is not None:
                    validator_indices[pubkey] = index
                    newly_activated += 1
                    logging.info("Previously pending validator %s now activated with index %d", pubkey[:20], index)
                else:
                    still_pending.append(pubkey)

            if newly_activated > 0:
                print(f"Activated: {newly_activated} validators")
                logging.info("Newly activated validators: %d (was pending, now has index)", newly_activated)

            logging.info("Still pending activation: %d validators", len(still_pending))
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
                exit_details = self.cache.get('exit_details', {})

                for pubkey, index in validator_indices.items():
                    if index in exited_statuses and pubkey not in exited_pubkeys:
                        for operator, pubkeys in operator_pubkeys.items():
                            if pubkey in pubkeys:
                                exit_info = exited_statuses[index]
                                operator_exited[operator] += 1
                                new_exits += 1
                                exited_pubkeys.add(pubkey)
                                
                                # Store detailed exit information
                                exit_details[pubkey] = {
                                    'validator_index': index,
                                    'operator': operator,
                                    'operator_name': self.format_operator_display(operator),
                                    'status': exit_info['status'],
                                    'exit_epoch': exit_info['exit_epoch'],
                                    'exit_timestamp': exit_info['exit_timestamp'],
                                    'withdrawable_epoch': exit_info['withdrawable_epoch'],
                                    'withdrawable_timestamp': exit_info['withdrawable_timestamp'],
                                    'balance': exit_info['balance'],
                                    'effective_balance': exit_info['effective_balance'],
                                    'slashed': exit_info['slashed'],
                                    'discovered_timestamp': int(time.time())
                                }
                                
                                display_name = self.format_operator_display(operator)
                                timestamp_str = datetime.datetime.fromtimestamp(exit_info['exit_timestamp']).strftime('%Y-%m-%d %H:%M:%S') if exit_info['exit_timestamp'] else 'Unknown'
                                print(f"Exit: Validator {index} ({display_name}) - {exit_info['status']} at {timestamp_str}")
                                break

                self.cache['exited_pubkeys'] = list(exited_pubkeys)
                self.cache['exit_details'] = exit_details

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

    def check_performance(self):
        """Check validator attestation performance and store in cache."""
        # Check if performance was updated recently (1 hour = 3600 seconds)
        last_performance_update = self.cache.get('performance_last_updated', 0)
        current_time = int(time.time())
        
        if current_time - last_performance_update < 3600:  # 1 hour
            hours_since_update = (current_time - last_performance_update) // 3600
            logging.info("Performance data updated recently (%d hours ago), skipping", hours_since_update)
            print(f"Performance data updated {hours_since_update} hours ago, skipping API calls")
            # Return existing performance data from cache
            return list(self.cache.get('operator_performance', {}).items())
        
        print("Performance data is stale (>1 hour), fetching fresh data from beaconcha.in...")
        logging.info("Starting performance check - last update was %d hours ago", 
                    (current_time - last_performance_update) // 3600)
        
        beaconchain_conn = http.client.HTTPSConnection("beaconcha.in")
        validator_indices = dict(self.cache.get('validator_indices', {}))
        exited_pubkeys = set(self.cache.get('exited_pubkeys', []))

        pubkeys = [pk for pk in validator_indices.keys() if pk not in exited_pubkeys]

        batch_size = 100
        performance_results = {}
        api_call_count = 0

        for i in range(0, len(pubkeys), batch_size):
            print(f"Fetching range {i} to {i + batch_size}...")
            batch = pubkeys[i:i + batch_size]

            while True:
                try:
                    endpoint = f"/api/v1/validator/{','.join(map(str, batch))}/attestationefficiency"
                    beaconchain_conn.request("GET", endpoint)
                    res = beaconchain_conn.getresponse()

                    if res.status == 429:
                        print(f"Rate limit hit after {api_call_count} calls. Backing off for 60 seconds...")
                        logging.warning("Rate limit encountered, backing off for 60 seconds")
                        res.read()
                        res.close()
                        time.sleep(60)
                        api_call_count = 0
                        continue

                    if res.status != 200:
                        print(f"HTTP error {res.status} for batch {i} to {i + batch_size}")
                        res.read()
                        res.close()
                        break

                    data = res.read().decode("utf-8")
                    res.close()
                    api_call_count += 1

                    try:
                        parsed = json.loads(data)
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse JSON for batch {i} to {i + batch_size}: {e}")
                        break

                    status = parsed.get("status")
                    if status == "OK":
                        for entry in parsed.get("data", []):
                            index = entry.get("validatorindex")
                            efficiency = entry.get("attestation_efficiency")
                            percent = max(0, round((2 - efficiency) * 100, 2))
                            performance_results[index] = percent
                    else:
                        print(f"Batch failed for range {i} to {i + batch_size}")

                    break

                except Exception as e:
                    print(f"Error fetching performance for batch from {i} to {i + batch_size}: {e}")
                    break

        beaconchain_conn.close()

        results = []
        operator_pubkeys = defaultdict(list, self.cache.get('validator_pubkeys', {}))
        for operator in operator_pubkeys:
            total = 0
            count = 0
            for pubkey in operator_pubkeys[operator]:
                index = self._get_validator_index(pubkey)
                performance = performance_results.get(index, None)
                if performance is not None:
                    total += performance
                    count += 1

            if count > 0:
                results.append((operator, total / count))

        self.cache['operator_performance'] = dict(results)
        self.cache['performance_last_updated'] = current_time
        
        print(f"Performance data updated successfully for {len(results)} operators")
        logging.info("Performance check completed: %d operators updated", len(results))

        return results

    def generate_report(self, operator_validators: dict, operator_exited: dict,
                       total_validators: int, total_exited: int, operator_performance: dict) -> None:
        """Generate comprehensive validator status report with ENS names."""

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

        # Top operators with ENS names
        print("\n=== TOP OPERATORS ===")
        operator_list = [(addr, count) for addr, count in active_per_operator.items()]
        operator_list.sort(key=lambda x: x[1], reverse=True)

        for addr, active_count in operator_list[:5]:
            exited_count = operator_exited.get(addr, 0)
            total_ever = operator_validators.get(addr, 0)
            display_name = self.format_operator_display(addr)

            if exited_count > 0:
                print(f"{active_count} active ({total_ever} total, {exited_count} exited): {display_name}")
            else:
                print(f"{active_count} validators: {display_name}")

        # Fully exited operators
        fully_exited = [(op, cnt) for op, cnt in operator_validators.items()
                       if operator_exited.get(op, 0) == cnt and cnt > 0]

        if fully_exited:
            print("\n=== FULLY EXITED OPERATORS ===")
            for operator, count in fully_exited:
                display_name = self.format_operator_display(operator)
                print(f"All {count} validators exited: {display_name}")

        # Worst attestation performance
        print("\n=== WORST ATTESTATION PERFORMANCE ===")
        sorted_operator_performance = sorted(operator_performance, key=lambda x: (x[1] is None, x[1]))
        for operator, percent in list(reversed(sorted_operator_performance[:5])):
            display_name = self.format_operator_display(operator)
            percent_str = f"{percent}%" if percent is not None else "N/A"
            print(f"Operator: {display_name}, Efficiency: {percent_str}")

        # Best attestation performance
        print("\n=== BEST ATTESTATION PERFORMANCE ===")
        sorted_operator_performance = sorted(operator_performance, key=lambda x: (x[1] is None, -x[1] if x[1] is not None else float('inf')))
        for operator, percent in sorted_operator_performance[:5]:
            display_name = self.format_operator_display(operator)
            percent_str = f"{percent:.2f}%" if percent is not None else "N/A"
            print(f"Operator: {display_name}, Efficiency: {percent_str}")

        # Cost analysis summary
        operator_costs = self.cache.get('operator_costs', {})
        if operator_costs:
            print("\n=== COST ANALYSIS SUMMARY ===")
            total_cost = sum(cost['total_cost_eth'] for cost in operator_costs.values())
            total_txs = sum(cost['total_txs'] for cost in operator_costs.values())
            total_validators_created = sum(cost['total_validators_created'] for cost in operator_costs.values())
            operators_with_costs = len([c for c in operator_costs.values() if c['total_txs'] > 0])

            print(f"Total gas spent: {total_cost:.6f} ETH")
            print(f"Total transactions: {total_txs}")
            print(f"Total validators created: {total_validators_created}")
            print(f"Operators with transaction data: {operators_with_costs}")

            if total_txs > 0:
                avg_cost_per_tx = total_cost / total_txs
                print(f"Average cost per transaction: {avg_cost_per_tx:.6f} ETH")

            if total_validators_created > 0:
                avg_cost_per_validator = total_cost / total_validators_created
                print(f"Average cost per validator: {avg_cost_per_validator:.6f} ETH")

        # ENS Summary
        ens_names = self.cache.get('ens_names', {})
        ens_sources = self.cache.get('ens_sources', {})
        if ens_names:
            # Count sources
            manual_count = sum(1 for source in ens_sources.values() if source == 'manual')
            onchain_count = sum(1 for source in ens_sources.values() if source == 'on-chain')
            
            print(f"\n=== ENS NAMES RESOLVED ===")
            print(f"Total ENS names found: {len(ens_names)}")
            print(f"Manual ENS mappings: {manual_count}")
            print(f"On-chain ENS lookups: {onchain_count}")
            print(f"ENS coverage: {len(ens_names)}/{len(operator_validators)} operators ({len(ens_names)/len(operator_validators)*100:.1f}%)")

            # Show operators with ENS names
            ens_operators = [(addr, name) for addr, name in ens_names.items() if addr in operator_validators]
            if ens_operators:
                print("\nOperators with ENS names:")
                for addr, name in sorted(ens_operators, key=lambda x: operator_validators.get(x[0], 0), reverse=True)[:10]:
                    validator_count = operator_validators.get(addr, 0)
                    source = ens_sources.get(addr, 'unknown')
                    source_label = f" ({source})" if source != 'unknown' else " (unknown)"
                    print(f"  {name}{source_label} ({addr[:8]}...{addr[-6:]}): {validator_count} validators")

    def query_clickhouse_active_exiting(self, validator_indices: List[int]) -> List[Dict]:
        """Query ClickHouse for validators in active_exiting state"""
        if not validator_indices:
            return []
        
        indices_str = ','.join(map(str, validator_indices))
        query = f"""
        SELECT 
            val_id,
            val_status,
            epoch,
            val_balance
        FROM default.validators_summary 
        WHERE val_id IN ({indices_str})
            AND val_status = 'active_exiting'
            AND epoch = (SELECT MAX(epoch) FROM default.validators_summary)
        """
        
        try:
            logging.info("Querying ClickHouse for active_exiting validators")
            response = requests.post(self.clickhouse_url, data=query, timeout=30)
            response.raise_for_status()
            
            results = []
            for line in response.text.strip().split('\n'):
                if line:
                    parts = line.split('\t')
                    if len(parts) >= 4:
                        results.append({
                            'validator_index': int(parts[0]),
                            'status': parts[1],
                            'epoch': int(parts[2]),
                            'balance': int(parts[3]) if parts[3] != '\\N' else None
                        })
            
            logging.info("Found %d validators in active_exiting state from ClickHouse", len(results))
            return results
            
        except Exception as e:
            logging.error("Error querying ClickHouse for active_exiting validators: %s", str(e))
            return []
    
    def track_active_exiting_validators(self):
        """Track validators in active_exiting state and store in cache"""
        validator_indices = self.cache.get('validator_indices', {})
        if not validator_indices:
            logging.warning("No validator indices available for active_exiting check")
            return
        
        # Get list of validator indices
        indices_list = list(validator_indices.values())
        
        # Query ClickHouse for active_exiting validators
        active_exiting_data = self.query_clickhouse_active_exiting(indices_list)
        
        if not active_exiting_data:
            print("No validators found in active_exiting state")
            return
        
        print(f"Found {len(active_exiting_data)} validators in active_exiting state")
        
        # Initialize active_exiting_details if not exists
        if 'active_exiting_details' not in self.cache:
            self.cache['active_exiting_details'] = {}
        
        # Get reverse mapping of index -> pubkey
        index_to_pubkey = {v: k for k, v in validator_indices.items()}
        
        # Process each active_exiting validator
        for validator_data in active_exiting_data:
            validator_index = validator_data['validator_index']
            epoch = validator_data['epoch']
            balance = validator_data['balance']
            
            # Find pubkey for this validator index
            pubkey = index_to_pubkey.get(validator_index)
            if not pubkey:
                logging.warning("No pubkey found for validator index %d", validator_index)
                continue
            
            # Find operator for this pubkey
            operator = None
            for op, pubkeys in self.cache.get('validator_pubkeys', {}).items():
                if pubkey in pubkeys:
                    operator = op
                    break
            
            if not operator:
                logging.warning("No operator found for validator %d (pubkey %s)", validator_index, pubkey[:20])
                continue
            
            # Get operator name
            operator_name = self.format_operator_display(operator)
            
            # Store active_exiting data
            self.cache['active_exiting_details'][pubkey] = {
                'validator_index': validator_index,
                'operator': operator,
                'operator_name': operator_name,
                'status': 'active_exiting',
                'active_exiting_epoch': epoch,
                'active_exiting_timestamp': self._epoch_to_timestamp(epoch),
                'balance': balance,
                'discovered_timestamp': int(time.time())
            }
            
            logging.info("Stored active_exiting data for validator %d (operator %s)", 
                        validator_index, operator_name)
        
        # Update cache counts
        self.cache['total_active_exiting'] = len(self.cache['active_exiting_details'])
        
        # Save updated cache
        self._save_cache()
        
        print(f"Stored {len(active_exiting_data)} active_exiting validators in cache")
        logging.info("Active exiting tracking completed: %d validators tracked", len(active_exiting_data))
    
    def check_active_exiting_transitions(self):
        """Check cached active_exiting validators to see if they've transitioned to exited status using ClickHouse"""
        active_exiting_details = self.cache.get('active_exiting_details', {})
        if not active_exiting_details:
            logging.info("No active_exiting validators to check for transitions")
            return
        
        print(f"Checking {len(active_exiting_details)} cached active_exiting validators for transitions...")
        logging.info("Checking %d cached active_exiting validators for transitions", len(active_exiting_details))
        
        # Get validator indices from active_exiting cache
        validator_indices = []
        pubkey_to_index = {}
        for pubkey, details in active_exiting_details.items():
            validator_index = details.get('validator_index')
            if validator_index:
                validator_indices.append(validator_index)
                pubkey_to_index[validator_index] = pubkey
        
        if not validator_indices:
            logging.warning("No validator indices found in active_exiting cache")
            return
        
        # Query ClickHouse directly for these validators to check their current status
        indices_str = ','.join(map(str, validator_indices))
        query = f"""
        SELECT 
            val_id,
            val_status,
            epoch,
            val_balance,
            val_effective_balance,
            val_slashed
        FROM default.validators_summary 
        WHERE val_id IN ({indices_str})
            AND epoch = (SELECT MAX(epoch) FROM default.validators_summary)
        """
        
        try:
            logging.info("Querying ClickHouse for cached active_exiting validators status")
            response = requests.post(self.clickhouse_url, data=query, timeout=30)
            
            if response.status_code != 200:
                logging.error("ClickHouse query failed with status %d", response.status_code)
                return
                
            lines = response.text.strip().split('\n')
            if not lines or not lines[0]:
                print("All cached active_exiting validators are still in active_exiting state")
                return
            
            # Process results and find transitions
            exited_statuses = {}
            still_active_exiting = 0
            
            for line in lines:
                parts = line.split('\t')
                if len(parts) >= 6:
                    val_id = int(parts[0])
                    status = parts[1]
                    epoch = int(parts[2])
                    balance = int(parts[3]) if parts[3] != '\\N' else None
                    effective_balance = int(parts[4]) if parts[4] != '\\N' else None
                    slashed = bool(int(parts[5])) if parts[5] != '\\N' else False
                    
                    if status in ['exited_unslashed', 'exited_slashed', 'withdrawal_possible', 'withdrawal_done']:
                        # This validator has transitioned to exited state
                        exit_info = {
                            'status': status,
                            'exit_epoch': epoch,
                            'withdrawable_epoch': None,
                            'exit_timestamp': self._epoch_to_timestamp(epoch),
                            'withdrawable_timestamp': None,
                            'balance': balance,
                            'effective_balance': effective_balance,
                            'slashed': slashed
                        }
                        exited_statuses[val_id] = exit_info
                    elif status == 'active_exiting':
                        still_active_exiting += 1
        
        except Exception as e:
            logging.error("Error querying ClickHouse for active_exiting transitions: %s", str(e))
            return
        
        if not exited_statuses:
            print(f"All {len(active_exiting_details)} cached active_exiting validators are still in active_exiting state")
            return
        
        # Process transitions from active_exiting to exited
        print(f"Found {len(exited_statuses)} validators that have transitioned from active_exiting to exited")
        
        # Initialize exit tracking data structures
        exited_pubkeys = set(self.cache.get('exited_pubkeys', []))
        exit_details = self.cache.get('exit_details', {})
        operator_exited = defaultdict(int)
        for operator in self.cache.get('validator_pubkeys', {}):
            operator_exited[operator] = len([pk for pk in exited_pubkeys 
                                           if pk in self.cache.get('validator_pubkeys', {}).get(operator, [])])
        
        transitions_count = 0
        
        for validator_index, exit_info in exited_statuses.items():
            pubkey = pubkey_to_index.get(validator_index)
            if pubkey and pubkey in active_exiting_details:
                # This validator has transitioned from active_exiting to exited
                active_exiting_data = active_exiting_details[pubkey]
                operator = active_exiting_data.get('operator')
                
                # Add to exited tracking
                if pubkey not in exited_pubkeys:
                    exited_pubkeys.add(pubkey)
                    if operator:
                        operator_exited[operator] += 1
                    transitions_count += 1
                
                # Store detailed exit information
                exit_details[pubkey] = {
                    'validator_index': validator_index,
                    'operator': operator,
                    'operator_name': active_exiting_data.get('operator_name'),
                    'exit_epoch': exit_info.get('exit_epoch'),
                    'exit_timestamp': exit_info.get('exit_timestamp'),
                    'withdrawable_epoch': exit_info.get('withdrawable_epoch'),
                    'withdrawable_timestamp': exit_info.get('withdrawable_timestamp'),
                    'balance': exit_info.get('balance'),
                    'effective_balance': exit_info.get('effective_balance'),
                    'slashed': exit_info.get('slashed', False),
                    'status': exit_info.get('status'),
                    'transition_from_active_exiting': True,
                    'active_exiting_epoch': active_exiting_data.get('active_exiting_epoch'),
                    'discovered_timestamp': int(time.time())
                }
                
                # Remove from active_exiting cache
                del active_exiting_details[pubkey]
                
                logging.info("Validator %d transitioned from active_exiting to %s (operator: %s)", 
                           validator_index, exit_info.get('status'), 
                           active_exiting_data.get('operator_name', 'unknown'))
        
        # Update cache with new data
        self.cache['exited_pubkeys'] = list(exited_pubkeys)
        self.cache['exit_details'] = exit_details
        self.cache['active_exiting_details'] = active_exiting_details
        self.cache['total_active_exiting'] = len(active_exiting_details)
        
        # Update operator exit counts
        for operator, count in operator_exited.items():
            if count > 0:
                self.cache.setdefault('operator_exited', {})[operator] = count
        
        self._save_cache()
        
        if transitions_count > 0:
            print(f"Updated {transitions_count} validators from active_exiting to exited status")
            logging.info("Processed %d transitions from active_exiting to exited", transitions_count)
        else:
            print("No new transitions found")
    
    def _epoch_to_timestamp(self, epoch: int) -> int:
        """Convert beacon chain epoch to Unix timestamp"""
        return GENESIS_TIME + (epoch * SLOTS_PER_EPOCH * SECONDS_PER_SLOT)

    def generate_dashboard_exit_data(self) -> dict:
        """Generate dashboard-friendly exit data with timestamps and details."""
        exit_details = self.cache.get('exit_details', {})
        operator_validators = self.cache.get('operator_validators', {})
        operator_exited = self.cache.get('operator_exited', {})
        ens_names = self.cache.get('ens_names', {})
        ens_sources = self.cache.get('ens_sources', {})
        
        # Count ENS sources
        manual_count = sum(1 for source in ens_sources.values() if source == 'manual')
        onchain_count = sum(1 for source in ens_sources.values() if source == 'on-chain')
        
        dashboard_data = {
            'exit_summary': {
                'total_exited': sum(operator_exited.values()),
                'total_active': sum(operator_validators.values()) - sum(operator_exited.values()),
                'exit_rate_percent': (sum(operator_exited.values()) / sum(operator_validators.values()) * 100) if sum(operator_validators.values()) > 0 else 0,
                'last_updated': int(time.time())
            },
            'ens_summary': {
                'total_ens_names': len(ens_names),
                'manual_ens_count': manual_count,
                'onchain_ens_count': onchain_count,
                'ens_coverage_percent': (len(ens_names) / len(operator_validators) * 100) if len(operator_validators) > 0 else 0
            },
            'operators_with_exits': [],
            'recent_exits': [],
            'exit_timeline': []
        }
        
        # Operators with exits table data
        for operator, exit_count in operator_exited.items():
            if exit_count > 0:
                total_ever = operator_validators.get(operator, 0)
                still_active = total_ever - exit_count
                exit_rate = (exit_count / total_ever * 100) if total_ever > 0 else 0
                
                # Find most recent exit for this operator
                operator_exits = [exit_info for pubkey, exit_info in exit_details.items() 
                                if exit_info['operator'] == operator]
                latest_exit_timestamp = None
                if operator_exits:
                    latest_exit_timestamp = max([exit['exit_timestamp'] for exit in operator_exits 
                                               if exit['exit_timestamp']])
                
                operator_data = {
                    'operator': operator,
                    'operator_name': self.format_operator_display(operator),
                    'exits': exit_count,
                    'still_active': still_active,
                    'total_ever': total_ever,
                    'exit_rate': round(exit_rate, 1),
                    'latest_exit_timestamp': latest_exit_timestamp,
                    'latest_exit_date': datetime.datetime.fromtimestamp(latest_exit_timestamp).strftime('%Y-%m-%d') if latest_exit_timestamp else 'Data not available'
                }
                
                dashboard_data['operators_with_exits'].append(operator_data)
        
        # Sort by exit count descending
        dashboard_data['operators_with_exits'].sort(key=lambda x: x['exits'], reverse=True)
        
        # Recent exits (last 30 days)
        thirty_days_ago = int(time.time()) - (30 * 24 * 60 * 60)
        recent_exits = []
        
        for pubkey, exit_info in exit_details.items():
            if exit_info.get('exit_timestamp') and exit_info['exit_timestamp'] > thirty_days_ago:
                recent_exits.append({
                    'validator_index': exit_info['validator_index'],
                    'operator': exit_info['operator'],
                    'operator_name': exit_info['operator_name'],
                    'exit_timestamp': exit_info['exit_timestamp'],
                    'exit_date': datetime.datetime.fromtimestamp(exit_info['exit_timestamp']).strftime('%Y-%m-%d %H:%M:%S'),
                    'status': exit_info['status'],
                    'slashed': exit_info.get('slashed', False),
                    'balance_gwei': exit_info.get('balance'),
                    'exit_epoch': exit_info.get('exit_epoch')
                })
        
        # Sort by timestamp descending (most recent first)
        recent_exits.sort(key=lambda x: x['exit_timestamp'], reverse=True)
        dashboard_data['recent_exits'] = recent_exits[:50]  # Limit to 50 most recent
        
        # Exit timeline for charts (group by day)
        exit_timeline = defaultdict(lambda: {'voluntary': 0, 'slashed': 0, 'total': 0})
        
        for exit_info in exit_details.values():
            if exit_info.get('exit_timestamp'):
                exit_date = datetime.datetime.fromtimestamp(exit_info['exit_timestamp']).strftime('%Y-%m-%d')
                if exit_info.get('slashed'):
                    exit_timeline[exit_date]['slashed'] += 1
                else:
                    exit_timeline[exit_date]['voluntary'] += 1
                exit_timeline[exit_date]['total'] += 1
        
        # Convert to list and sort by date
        timeline_list = []
        for date, counts in exit_timeline.items():
            timeline_list.append({
                'date': date,
                'voluntary_exits': counts['voluntary'],
                'slashed_exits': counts['slashed'],
                'total_exits': counts['total']
            })
        
        timeline_list.sort(key=lambda x: x['date'])
        dashboard_data['exit_timeline'] = timeline_list
        
        return dashboard_data

    def run_analysis(self) -> None:
        """Execute complete validator analysis with ENS resolution."""
        try:
            print("NodeSet Validator Tracker with ENS Support and Manual Override")
            print(f"Ethereum node: Connected")
            print(f"Beacon API: {'Connected' if self.beacon_api_url else 'Disabled'}")
            print(f"Etherscan API: {'Enabled' if self.etherscan_api_key else 'Disabled'}")
            print(f"Manual ENS file: {MANUAL_ENS_FILE} ({len(self.manual_ens_names)} mappings loaded)")

            # Scan for validators
            operator_validators, total_validators = self.scan_validators()

            # Update ENS names (runs every hour)
            if operator_validators:
                print(f"\nUpdating ENS names for {len(operator_validators)} operators")
                self._update_ens_names()
                self._save_cache()

            # Track exits if beacon API available
            operator_exited, total_exited = {}, 0
            if self.beacon_api_url and total_validators > 0:
                print(f"\nTracking exits for {total_validators} validators")
                operator_exited, total_exited = self.track_exits()
                self._save_cache()

            # Check attestation performance
            print(f"\nChecking performance for {total_validators} validators")
            operator_performance = self.check_performance()
            self._save_cache()

            # Analyze transaction costs
            if self.etherscan_api_key:
                print(f"\nAnalyzing transaction costs")
                self.analyze_operator_costs()
                self._save_cache()

            # Generate report
            self.generate_report(operator_validators, operator_exited,
                               total_validators, total_exited, operator_performance)
            
            # Generate dashboard data with detailed exit information
            print("\nGenerating dashboard data...")
            dashboard_data = self.generate_dashboard_exit_data()
            
            # Save dashboard data to JSON file
            dashboard_file = "dashboard_exit_data.json"
            with open(dashboard_file, 'w') as f:
                json.dump(dashboard_data, f, indent=2)
            print(f"Dashboard data saved to {dashboard_file}")
            
            # Check if any cached active_exiting validators have transitioned to exited
            print("\nChecking active_exiting validators for transitions to exited status...")
            self.check_active_exiting_transitions()
            
            # Check ClickHouse for active_exiting validators
            print("\nChecking for active_exiting validators...")
            self.track_active_exiting_validators()
            
            logging.info("Analysis completed successfully")

        except Exception as e:
            logging.error("Analysis failed: %s", str(e))
            raise


def main():
    """Main execution function."""
    eth_client_url = os.getenv('ETH_CLIENT_URL')
    if not eth_client_url:
        raise ValueError("ETH_CLIENT_URL environment variable is required")

    beacon_api_url = os.getenv('BEACON_API_URL')
    etherscan_api_key = os.getenv('ETHERSCAN_API_KEY')

    tracker = NodeSetValidatorTracker(eth_client_url, beacon_api_url, etherscan_api_key)
    tracker.run_analysis()


if __name__ == "__main__":
    main()
