"""
NodeSet Block Proposal Tracker

"""

import os
import json
import time
import logging
import requests
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Set, Optional
from web3 import Web3
import datetime

# Configuration
logging.basicConfig(
    level=logging.INFO,
    filename='proposal_tracker.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
GENESIS_TIME = 1606824023
SECONDS_PER_SLOT = 12
SLOTS_PER_EPOCH = 32
DEPLOYMENT_BLOCK = 22318339

# Cache files
VALIDATOR_CACHE_FILE = "nodeset_validator_tracker_cache.json"
PROPOSAL_CACHE_FILE = "proposal_cache.json"
PROPOSAL_DATA_FILE = "proposals.json"

class BlockProposalTracker:
    """
    Tracks block proposals for NodeSet validators with value calculation.
    """

    def __init__(self, eth_client_url: str, beacon_api_url: str):
        self.web3 = self._setup_web3(eth_client_url)
        self.beacon_api_url = self._setup_beacon_api(beacon_api_url)
        self.cache = self._load_cache()
        self.validator_data = self._load_validator_data()
        self.tracked_validators = self._get_tracked_validators()

    def _setup_web3(self, eth_client_url: str) -> Web3:
        """Initialize Web3 connection."""
        web3 = Web3(Web3.HTTPProvider(eth_client_url))
        if not web3.is_connected():
            raise ConnectionError(f"Failed to connect to Ethereum node at {eth_client_url}")

        logging.info("Connected to Ethereum node. Latest block: %d", web3.eth.block_number)
        return web3

    def _setup_beacon_api(self, beacon_api_url: str) -> str:
        """Verify beacon API connectivity."""
        try:
            response = requests.get(f"{beacon_api_url}/eth/v1/node/health", timeout=10)
            if response.status_code in [200, 206]:
                print(f"Connected to beacon chain consensus client at {beacon_api_url}")
                if response.status_code == 206:
                    print("  Note: Beacon node reports partial sync status (206) but is functional")
                logging.info("Connected to beacon API at %s (status: %d)", beacon_api_url, response.status_code)
                return beacon_api_url
            else:
                raise ConnectionError(f"Beacon API health check failed: {response.status_code}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to beacon API: {str(e)}")

    def _load_cache(self) -> dict:
        """Load proposal tracking cache."""
        if os.path.exists(PROPOSAL_CACHE_FILE):
            try:
                with open(PROPOSAL_CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    logging.info("Loaded proposal cache: last slot %d", cache.get('last_slot', 0))
                    return cache
            except Exception as e:
                logging.warning("Error loading proposal cache: %s", str(e))

        return {
            'last_slot': self._get_deployment_slot(),
            'proposals_found': 0,
            'last_updated': 0
        }

    def _get_deployment_slot(self) -> int:
        """Calculate approximate slot for deployment block."""
        deployment_time = 1730000000  # Approximate timestamp for deployment
        return max(0, (deployment_time - GENESIS_TIME) // SECONDS_PER_SLOT)

    def _load_validator_data(self) -> dict:
        """Load validator data from main tracker."""
        if not os.path.exists(VALIDATOR_CACHE_FILE):
            raise FileNotFoundError(f"Validator cache file not found: {VALIDATOR_CACHE_FILE}")

        try:
            with open(VALIDATOR_CACHE_FILE, 'r') as f:
                data = json.load(f)
                logging.info("Loaded validator data: %d total validators", data.get('total_validators', 0))
                return data
        except Exception as e:
            raise Exception(f"Error loading validator data: {str(e)}")

    def _get_tracked_validators(self) -> Dict[int, dict]:
        """Get active validators to track."""
        tracked = {}
        
        validator_indices = self.validator_data.get('validator_indices', {})
        validator_pubkeys = self.validator_data.get('validator_pubkeys', {})
        exited_pubkeys = set(self.validator_data.get('exited_pubkeys', []))
        ens_names = self.validator_data.get('ens_names', {})

        for operator, pubkeys in validator_pubkeys.items():
            for pubkey in pubkeys:
                if pubkey in exited_pubkeys:
                    continue

                validator_index = validator_indices.get(pubkey)
                if validator_index is not None:
                    tracked[validator_index] = {
                        'pubkey': pubkey,
                        'operator': operator,
                        'ens_name': ens_names.get(operator)
                    }

        logging.info("Tracking %d active validators", len(tracked))
        return tracked

    def _save_cache(self) -> None:
        """Save proposal tracking cache."""
        try:
            self.cache['last_updated'] = int(time.time())
            with open(PROPOSAL_CACHE_FILE, 'w') as f:
                json.dump(self.cache, f, indent=2)
            logging.info("Cache saved: %d proposals found", self.cache['proposals_found'])
        except Exception as e:
            logging.error("Error saving cache: %s", str(e))

    def _get_current_slot(self) -> int:
        """Get current beacon chain slot."""
        try:
            response = requests.get(f"{self.beacon_api_url}/eth/v1/beacon/headers/head", timeout=10)
            if response.status_code == 200:
                data = response.json()
                return int(data['data']['header']['message']['slot'])
        except Exception as e:
            logging.debug("Error getting current slot: %s", str(e))

        current_time = int(time.time())
        return (current_time - GENESIS_TIME) // SECONDS_PER_SLOT

    def _slot_to_timestamp(self, slot: int) -> int:
        """Convert slot to timestamp."""
        return GENESIS_TIME + (slot * SECONDS_PER_SLOT)

    def _get_block_details(self, slot: int) -> Optional[dict]:
        """Get block details for slot."""
        try:
            response = requests.get(f"{self.beacon_api_url}/eth/v2/beacon/blocks/{slot}", timeout=30)
            
            if response.status_code == 200:
                return response.json()['data']
            elif response.status_code == 404:
                return None
            else:
                logging.debug("Error fetching block %d: HTTP %d", slot, response.status_code)
                return None
        except Exception as e:
            logging.debug("Error fetching block %d: %s", slot, str(e))
            return None

    def _get_execution_block(self, block_number: int) -> Optional[dict]:
        """Get execution layer block."""
        try:
            return self.web3.eth.get_block(block_number, full_transactions=False)
        except Exception as e:
            logging.debug("Error fetching execution block %d: %s", block_number, str(e))
            return None

    def _calculate_block_value(self, execution_block: dict) -> Tuple[int, float, dict]:
        """Calculate total block value and components."""
        try:
            gas_used = execution_block.get('gasUsed', 0)
            gas_limit = execution_block.get('gasLimit', 0)
            base_fee = execution_block.get('baseFeePerGas', 0)
            tx_count = len(execution_block.get('transactions', []))

            # Basic consensus reward estimate
            consensus_reward_wei = 20000 * 1e9 // 8  # Rough approximation

            # Execution layer fees
            execution_fees_wei = gas_used * base_fee if gas_used and base_fee else 0

            total_value_wei = consensus_reward_wei + execution_fees_wei
            total_value_eth = total_value_wei / 1e18

            details = {
                'gas_used': gas_used,
                'gas_limit': gas_limit,
                'base_fee': base_fee,
                'tx_count': tx_count,
                'gas_utilization': (gas_used / gas_limit * 100) if gas_limit > 0 else 0,
                'consensus_reward_eth': consensus_reward_wei / 1e18,
                'execution_fees_eth': execution_fees_wei / 1e18
            }

            return total_value_wei, total_value_eth, details

        except Exception as e:
            logging.debug("Error calculating block value: %s", str(e))
            return 0, 0.0, {}

    def _format_operator_name(self, validator_info: dict) -> str:
        """Format operator display name."""
        ens_name = validator_info.get('ens_name')
        operator = validator_info['operator']
        
        if ens_name:
            return f"{ens_name} ({operator[:8]}...{operator[-6:]})"
        else:
            return f"{operator[:8]}...{operator[-6:]}"

    def _load_existing_proposals(self) -> List[dict]:
        """Load existing proposal data."""
        if os.path.exists(PROPOSAL_DATA_FILE):
            try:
                with open(PROPOSAL_DATA_FILE, 'r') as f:
                    data = json.load(f)
                    proposals = data.get('proposals', [])
                    logging.info("Loaded %d existing proposals", len(proposals))
                    return proposals
            except Exception as e:
                logging.warning("Error loading existing proposals: %s", str(e))
        return []

    def _save_proposals(self, proposals: List[dict]) -> None:
        """Save proposal data to file."""
        try:
            operator_stats = defaultdict(lambda: {'count': 0, 'total_value': 0})
            
            for proposal in proposals:
                op = proposal['operator']
                operator_stats[op]['count'] += 1
                operator_stats[op]['total_value'] += proposal['total_value_eth']

            data = {
                'metadata': {
                    'last_updated': datetime.datetime.now().isoformat(),
                    'total_proposals': len(proposals),
                    'total_value_eth': sum(p['total_value_eth'] for p in proposals),
                    'operators_tracked': len(operator_stats)
                },
                'operator_summary': {
                    op: {
                        'proposal_count': stats['count'],
                        'total_value_eth': stats['total_value'],
                        'average_value_eth': stats['total_value'] / stats['count']
                    }
                    for op, stats in operator_stats.items()
                },
                'proposals': proposals
            }

            with open(PROPOSAL_DATA_FILE, 'w') as f:
                json.dump(data, f, indent=2)

            total_value = sum(p['total_value_eth'] for p in proposals)
            logging.info("Saved %d proposals totaling %.6f ETH", len(proposals), total_value)

        except Exception as e:
            logging.error("Error saving proposals: %s", str(e))

    def scan_proposals(self, max_slots: Optional[int] = None) -> int:
        """Scan for new block proposals."""
        start_slot = self.cache['last_slot'] + 1
        current_slot = self._get_current_slot()
        
        if max_slots:
            end_slot = min(current_slot, start_slot + max_slots)
        else:
            end_slot = current_slot

        if start_slot > end_slot:
            print("No new slots to scan")
            logging.info("No new slots to scan")
            return 0

        total_slots = end_slot - start_slot + 1
        print(f"Scanning slots {start_slot:,} to {end_slot:,} ({total_slots:,} slots)")
        print(f"Tracking {len(self.tracked_validators)} validators")
        if self.cache['last_slot'] > 0:
            print(f"Resuming from slot {self.cache['last_slot']:,}")
            print(f"Previous proposals found: {self.cache['proposals_found']}")

        proposals_found = 0
        slots_processed = 0
        api_calls = 0
        skipped_slots = 0
        existing_proposals = self._load_existing_proposals()

        chunk_size = 10000
        for chunk_start in range(start_slot, end_slot + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, end_slot)
            
            print(f"Processing slots {chunk_start:,} to {chunk_end:,}")
            
            for slot in range(chunk_start, chunk_end + 1):
                try:
                    block_data = self._get_block_details(slot)
                    api_calls += 1
                    slots_processed += 1

                    if block_data is None:
                        skipped_slots += 1
                        continue

                    proposer_index = int(block_data['message']['proposer_index'])

                    if proposer_index in self.tracked_validators:
                        validator_info = self.tracked_validators[proposer_index]
                        
                        execution_payload = block_data['message']['body']['execution_payload']
                        block_number = int(execution_payload['block_number'])
                        execution_block = self._get_execution_block(block_number)

                        if execution_block:
                            total_value_wei, total_value_eth, details = self._calculate_block_value(execution_block)
                            
                            epoch = slot // SLOTS_PER_EPOCH
                            timestamp = self._slot_to_timestamp(slot)
                            date_str = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
                            
                            proposal = {
                                'slot': slot,
                                'epoch': epoch,
                                'block_number': block_number,
                                'timestamp': timestamp,
                                'date': date_str,
                                'validator_index': proposer_index,
                                'validator_pubkey': validator_info['pubkey'],
                                'operator': validator_info['operator'],
                                'operator_name': self._format_operator_name(validator_info),
                                'total_value_wei': total_value_wei,
                                'total_value_eth': total_value_eth,
                                **details
                            }

                            existing_proposals.append(proposal)
                            proposals_found += 1

                            operator_display = self._format_operator_name(validator_info)
                            print(f"  Proposal: slot {slot:,}, block {block_number:,}, validator {proposer_index} ({operator_display})")
                            print(f"    Value: {total_value_eth:.6f} ETH, Gas: {details.get('gas_used', 0):,}/{details.get('gas_limit', 0):,} ({details.get('gas_utilization', 0):.1f}%)")
                            logging.info("Proposal: slot %d, validator %d, value %.6f ETH", slot, proposer_index, total_value_eth)

                    if slots_processed % 25000 == 0:
                        print(f"  Progress: {slots_processed:,}/{total_slots:,} slots ({slots_processed/total_slots*100:.1f}%), {proposals_found} proposals, {api_calls} API calls")
                        
                        # Save intermediate progress
                        self.cache.update({
                            'last_slot': slot,
                            'proposals_found': self.cache['proposals_found'] + proposals_found
                        })
                        self._save_cache()

                except Exception as e:
                    logging.debug("Error processing slot %d: %s", slot, str(e))
                    continue

            chunk_proposals = len([p for p in existing_proposals if chunk_start <= p['slot'] <= chunk_end])
            print(f"Chunk complete: {chunk_proposals} proposals found in slots {chunk_start:,}-{chunk_end:,}")

        self.cache.update({
            'last_slot': end_slot,
            'proposals_found': self.cache['proposals_found'] + proposals_found
        })

        self._save_cache()
        if existing_proposals:
            self._save_proposals(existing_proposals)

        print(f"\nScan complete:")
        print(f"  Slots processed: {slots_processed:,}")
        print(f"  Skipped slots (no block): {skipped_slots:,}")
        print(f"  Proposals found: {proposals_found}")
        print(f"  API calls made: {api_calls:,}")
        print(f"  Total proposals tracked: {len(existing_proposals)}")
        
        return proposals_found

    def generate_report(self) -> None:
        """Generate proposal summary report."""
        if not os.path.exists(PROPOSAL_DATA_FILE):
            print("No proposal data available")
            return

        try:
            with open(PROPOSAL_DATA_FILE, 'r') as f:
                data = json.load(f)

            metadata = data.get('metadata', {})
            operator_summary = data.get('operator_summary', {})
            proposals = data.get('proposals', [])

            print("\n" + "="*60)
            print("NODESET BLOCK PROPOSAL REPORT")
            print("="*60)

            total_proposals = metadata.get('total_proposals', 0)
            total_value = metadata.get('total_value_eth', 0)
            operators_count = metadata.get('operators_tracked', 0)

            print(f"\nTotal proposals: {total_proposals:,}")
            print(f"Total value: {total_value:.6f} ETH")
            print(f"Operators with proposals: {operators_count}")

            if operator_summary:
                print(f"\n=== TOP OPERATORS BY PROPOSALS ===")
                sorted_operators = sorted(
                    operator_summary.items(),
                    key=lambda x: x[1]['proposal_count'],
                    reverse=True
                )

                for i, (operator, stats) in enumerate(sorted_operators[:10]):
                    ens_name = self.validator_data.get('ens_names', {}).get(operator)
                    display_name = f"{ens_name} ({operator[:8]}...)" if ens_name else f"{operator[:8]}..."
                    
                    count = stats['proposal_count']
                    value = stats['total_value_eth']
                    avg = stats['average_value_eth']

                    print(f"{count:3d} proposals: {display_name} (total: {value:.6f} ETH, avg: {avg:.6f} ETH)")

            if proposals:
                print(f"\n=== RECENT PROPOSALS ===")
                recent = sorted(proposals, key=lambda x: x['slot'], reverse=True)[:5]

                for proposal in recent:
                    slot = proposal['slot']
                    date = proposal['date']
                    operator_name = proposal['operator_name']
                    value = proposal['total_value_eth']
                    gas_util = proposal.get('gas_utilization', 0)

                    print(f"Slot {slot:,} ({date}): {operator_name}")
                    print(f"  Value: {value:.6f} ETH, Gas utilization: {gas_util:.1f}%")

        except Exception as e:
            logging.error("Error generating report: %s", str(e))


def main():
    """Main execution function."""
    eth_client_url = os.getenv('ETH_CLIENT_URL')
    beacon_api_url = os.getenv('BEACON_API_URL')

    if not eth_client_url:
        raise ValueError("ETH_CLIENT_URL environment variable is required")
    if not beacon_api_url:
        raise ValueError("BEACON_API_URL environment variable is required")

    tracker = BlockProposalTracker(eth_client_url, beacon_api_url)
    proposals_found = tracker.scan_proposals()
    tracker.generate_report()


if __name__ == "__main__":
    main()
