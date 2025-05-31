"""
NodeSet Validator Tracker

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
AGGREGATE_SIGNATURE = "0x252dba42"

# Beacon chain constants
GENESIS_TIME = 1606824023
SECONDS_PER_SLOT = 12
SLOTS_PER_EPOCH = 32

class NodeSetValidatorTracker:
    """
    Tracks NodeSet validators from blockchain deposits through their complete lifecycle.
    """

    def __init__(self, eth_client_url: str, beacon_api_url: Optional[str] = None, etherscan_api_key: Optional[str] = None):
        self.web3 = self._setup_web3(eth_client_url)
        self.beacon_api_url = self._setup_beacon_api(beacon_api_url)
        self.etherscan_api_key = etherscan_api_key
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
            'processed_transactions': [],
            'operator_performance': {},
            'performance_last_updated': 0,
            'operator_transactions': {},
            'operator_costs': {},
            'cost_last_updated': 0
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
                    time.sleep(0.1)

            except Exception as e:
                logging.debug("Error checking validator batch: %s", str(e))
                continue

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
            print(f"Fetching costs for operator {i+1}/{len(operator_validators)}: {operator[:10]}...")
            
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
                           operator[:10], len(transactions), total_cost, total_validators_created)
            
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
        operator_exited = defaultdict(int, self.cache.get('exited_validators', {}))
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

    def check_performance(self):
        """Check validator attestation performance and store in cache."""
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
        self.cache['performance_last_updated'] = int(time.time())

        return results

    def generate_report(self, operator_validators: dict, operator_exited: dict,
                       total_validators: int, total_exited: int, operator_performance: dict) -> None:
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

        # Worst attestation performance
        print("\n=== WORST ATTESTATION PERFORMANCE ===")
        sorted_operator_performance = sorted(operator_performance, key=lambda x: (x[1] is None, x[1]))
        for operator, percent in list(reversed(sorted_operator_performance[:5])):
            percent_str = f"{percent}%" if percent is not None else "N/A"
            print(f"Operator: {operator}, Efficiency: {percent_str}")

        # Best attestation performance
        print("\n=== BEST ATTESTATION PERFORMANCE ===")
        sorted_operator_performance = sorted(operator_performance, key=lambda x: (x[1] is None, -x[1] if x[1] is not None else float('inf')))
        for operator, percent in sorted_operator_performance[:5]:
            percent_str = f"{percent:.2f}%" if percent is not None else "N/A"
            print(f"Operator: {operator}, Efficiency: {percent_str}")

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

    def run_analysis(self) -> None:
        """Execute complete validator analysis."""
        try:
            print("NodeSet Validator Tracker")
            print(f"Ethereum node: Connected")
            print(f"Beacon API: {'Connected' if self.beacon_api_url else 'Disabled'}")
            print(f"Etherscan API: {'Enabled' if self.etherscan_api_key else 'Disabled'}")

            # Scan for validators
            operator_validators, total_validators = self.scan_validators()

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
