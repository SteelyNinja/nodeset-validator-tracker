"""
NodeSet Block Proposal Tracker - Comprehensive Version with Graffiti Analysis
Combines local clients with external APIs for complete reward tracking and consensus client detection

Data Sources:
1. Local: Lighthouse beacon + pruned execution client
2. External: Beaconcha.in API for execution/MEV rewards
3. Graffiti: Consensus client identification from block graffiti
4. Fallbacks: Local analysis for reliability
"""

import os
import json
import time
import logging
import requests
import re
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Set, Optional
from web3 import Web3
import datetime
from dataclasses import dataclass
import decimal

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to avoid scientific notation for small numbers."""
    def encode(self, obj):
        if isinstance(obj, float):
            # Format floats to avoid scientific notation
            return format(obj, 'f')
        return super(DecimalEncoder, self).encode(obj)
    
    def iterencode(self, obj, _one_shot=False):
        """Encode object, avoiding scientific notation."""
        if isinstance(obj, float):
            yield format(obj, 'f')
        else:
            for chunk in super(DecimalEncoder, self).iterencode(obj, _one_shot):
                yield chunk

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

# API Configuration
BEACONCHAIN_API_BASE = "https://beaconcha.in/api/v1"

@dataclass
class RewardComponents:
    """Structure for organizing reward components"""
    consensus_wei: int = 0
    execution_wei: int = 0  # Total execution layer rewards (including MEV)
    mev_wei: int = 0       # MEV breakdown (subset of execution_wei)
    total_wei: int = 0
    data_sources: List[str] = None
    
    def __post_init__(self):
        if self.data_sources is None:
            self.data_sources = []
        # Total = consensus + execution (MEV is already included in execution)
        self.total_wei = self.consensus_wei + self.execution_wei

class GraffitiAnalyzer:
    """
    Analyzes beacon block graffiti to identify consensus clients
    """
    
    def __init__(self):
        # Client signature patterns (case-insensitive)
        self.client_patterns = {
            'lighthouse': [
                r'lighthouse', r'sigp', r'sigma.*prime', r'lh/', r'lighthouse/v'
            ],
            'prysm': [
                r'prysm', r'prysmatic', r'prysmaticlabs', r'prysm/v'
            ],
            'teku': [
                r'teku', r'consensys', r'pegasys', r'artemis', r'teku/v'
            ],
            'nimbus': [
                r'nimbus', r'status\.im', r'nim-beacon', r'nimbus/v'
            ],
            'lodestar': [
                r'lodestar', r'chainsafe', r'lodestar/v'
            ],
            'grandine': [
                r'grandine', r'grandine/v'
            ]
        }
        
        # Common pool/service signatures that might mask client info
        self.pool_patterns = [
            r'rocketpool', r'rocket.*pool', r'rp', r'stakewise', r'lido',
            r'coinbase', r'kraken', r'binance', r'ethereum.*on.*arm'
        ]

    def _decode_graffiti(self, graffiti_hex: str) -> str:
        """
        Decode graffiti from hex to readable string
        """
        try:
            if graffiti_hex.startswith('0x'):
                graffiti_hex = graffiti_hex[2:]
            
            # Remove trailing zeros
            graffiti_hex = graffiti_hex.rstrip('0')
            if len(graffiti_hex) % 2 != 0:
                graffiti_hex += '0'
            
            # Convert hex to bytes and decode
            graffiti_bytes = bytes.fromhex(graffiti_hex)
            graffiti_text = graffiti_bytes.decode('utf-8', errors='ignore').strip()
            
            return graffiti_text
        
        except Exception:
            return graffiti_hex  # Return original if decoding fails

    def identify_client(self, graffiti: str) -> Optional[str]:
        """
        Identify consensus client from graffiti text
        """
        if not graffiti:
            return None
            
        graffiti_lower = graffiti.lower()
        
        # Check each client pattern
        for client, patterns in self.client_patterns.items():
            for pattern in patterns:
                if re.search(pattern, graffiti_lower):
                    return client
        
        return None

    def is_pool_signature(self, graffiti: str) -> bool:
        """
        Check if graffiti contains pool/service signature
        """
        if not graffiti:
            return False
            
        graffiti_lower = graffiti.lower()
        for pattern in self.pool_patterns:
            if re.search(pattern, graffiti_lower):
                return True
        return False

    def extract_version_info(self, graffiti: str) -> Optional[str]:
        """
        Extract version information from graffiti
        """
        version_patterns = [
            r'v?(\d+\.\d+\.\d+(?:-\w+)?)',
            r'version[:\s]+(\d+\.\d+\.\d+)',
            r'/v(\d+\.\d+\.\d+)'
        ]
        
        for pattern in version_patterns:
            match = re.search(pattern, graffiti, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None

    def analyze_graffiti(self, graffiti_hex: str) -> Dict[str, Optional[str]]:
        """
        Complete graffiti analysis returning all extracted information
        """
        graffiti_text = self._decode_graffiti(graffiti_hex)
        
        return {
            'graffiti_text': graffiti_text,
            'client': self.identify_client(graffiti_text),
            'version': self.extract_version_info(graffiti_text),
            'is_pool': self.is_pool_signature(graffiti_text)
        }

class ComprehensiveProposalTracker:
    """
    Comprehensive block proposal tracker using multiple data sources for extended periods.
    Now includes graffiti analysis for consensus client identification.
    """

    def __init__(self, eth_client_url: str, beacon_api_url: str, 
                 enable_external_apis: bool = True):
        self.web3 = self._setup_web3(eth_client_url)
        self.beacon_api_url = self._setup_beacon_api(beacon_api_url)
        self.enable_external_apis = enable_external_apis
        self.graffiti_analyzer = GraffitiAnalyzer()
        
        self.cache = self._load_cache()
        self.validator_data = self._load_validator_data()
        self.tracked_validators = self._get_tracked_validators()
        
        # API rate limiting
        self.last_beaconchain_call = 0
        self.beaconchain_rate_limit = 0.1  # 10 calls per second max

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
                logging.info("Connected to beacon API at %s", beacon_api_url)
                return beacon_api_url
            else:
                raise ConnectionError(f"Beacon API health check failed: {response.status_code}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to beacon API: {str(e)}")

    def _rate_limit_wait(self, api_type: str) -> None:
        """Implement rate limiting for external APIs."""
        current_time = time.time()
        
        if api_type == "beaconchain":
            time_since_last = current_time - self.last_beaconchain_call
            if time_since_last < self.beaconchain_rate_limit:
                time.sleep(self.beaconchain_rate_limit - time_since_last)
            self.last_beaconchain_call = time.time()

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
            'last_updated': 0,
            'client_diversity_stats': {}
        }

    def _get_deployment_slot(self) -> int:
        """Get the starting slot for NodeSet project."""
        return 11594665  # NodeSet project start slot

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
                json.dump(self.cache, f, indent=2, cls=DecimalEncoder)
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

    def _extract_graffiti_data(self, beacon_block: dict) -> dict:
        """Extract and analyze graffiti from beacon block."""
        try:
            graffiti_hex = beacon_block['message']['body']['graffiti']
            graffiti_analysis = self.graffiti_analyzer.analyze_graffiti(graffiti_hex)
            
            return {
                'graffiti_hex': graffiti_hex,
                'graffiti_text': graffiti_analysis['graffiti_text'],
                'consensus_client': graffiti_analysis['client'],
                'client_version': graffiti_analysis['version'],
                'has_pool_signature': graffiti_analysis['is_pool']
            }
            
        except Exception as e:
            logging.debug("Error extracting graffiti: %s", str(e))
            return {
                'graffiti_hex': '',
                'graffiti_text': '',
                'consensus_client': None,
                'client_version': None,
                'has_pool_signature': False
            }

    def _get_consensus_rewards_local(self, slot: int, validator_index: int) -> Tuple[int, dict]:
        """Get consensus rewards from local beacon API."""
        try:
            response = requests.get(
                f"{self.beacon_api_url}/eth/v1/beacon/rewards/blocks/{slot}",
                timeout=30
            )
            
            if response.status_code == 200:
                rewards_data = response.json()['data']
                
                proposer_slashings = int(rewards_data.get('proposer_slashings', '0'))
                attester_slashings = int(rewards_data.get('attester_slashings', '0'))
                attestations = int(rewards_data.get('attestations', '0'))
                deposits = int(rewards_data.get('deposits', '0'))
                sync_aggregate = int(rewards_data.get('sync_aggregate', '0'))
                
                total_consensus_gwei = (
                    proposer_slashings + attester_slashings + 
                    attestations + deposits + sync_aggregate
                )
                
                details = {
                    'proposer_slashings_gwei': proposer_slashings,
                    'attester_slashings_gwei': attester_slashings,
                    'attestations_gwei': attestations,
                    'deposits_gwei': deposits,
                    'sync_aggregate_gwei': sync_aggregate,
                    'total_consensus_gwei': total_consensus_gwei,
                    'data_source': 'local_beacon_api'
                }
                
                return total_consensus_gwei * 1e9, details
                
        except Exception as e:
            logging.debug("Error getting local consensus rewards: %s", str(e))
        
        return 0, {}

    def _get_consensus_rewards_enhanced_local(self, slot: int, validator_index: int, beacon_block: dict) -> Tuple[int, dict]:
        """Enhanced consensus rewards calculation using local beacon data + network analysis."""
        try:
            epoch = slot // SLOTS_PER_EPOCH
            
            # Try to get precise rewards from local beacon API first
            local_rewards, local_details = self._get_consensus_rewards_local(slot, validator_index)
            
            if local_rewards > 0:
                return local_rewards, local_details
            
            # Enhanced estimation using beacon block data
            attestation_count = len(beacon_block['message']['body'].get('attestations', []))
            deposits_count = len(beacon_block['message']['body'].get('deposits', []))
            proposer_slashings_count = len(beacon_block['message']['body'].get('proposer_slashings', []))
            attester_slashings_count = len(beacon_block['message']['body'].get('attester_slashings', []))
            
            # Get sync aggregate participation
            sync_aggregate = beacon_block['message']['body'].get('sync_aggregate', {})
            sync_committee_bits = sync_aggregate.get('sync_committee_bits', '')
            sync_participation = 512  # Default to full participation
            
            if sync_committee_bits:
                try:
                    if isinstance(sync_committee_bits, str) and sync_committee_bits.startswith('0x'):
                        hex_val = int(sync_committee_bits, 16)
                        sync_participation = bin(hex_val).count('1')
                    else:
                        sync_participation = 512
                except Exception:
                    sync_participation = 512
            
            # Current mainnet consensus reward estimates
            base_proposer_reward_gwei = 15000
            attestation_reward_per_att = 8
            attestation_rewards = attestation_count * attestation_reward_per_att
            sync_reward_per_participant = 2
            sync_rewards = (sync_participation / 512) * sync_reward_per_participant * 512
            slashing_rewards = (proposer_slashings_count + attester_slashings_count) * 500000
            deposit_rewards = deposits_count * 1000
            
            total_consensus_gwei = (base_proposer_reward_gwei + attestation_rewards + 
                                  sync_rewards + slashing_rewards + deposit_rewards)
            
            details = {
                'attestation_count': attestation_count,
                'attestation_rewards_gwei': attestation_rewards,
                'deposits_count': deposits_count,
                'deposit_rewards_gwei': deposit_rewards,
                'proposer_slashings_count': proposer_slashings_count,
                'attester_slashings_count': attester_slashings_count,
                'slashing_rewards_gwei': slashing_rewards,
                'sync_participation': sync_participation,
                'sync_rewards_gwei': sync_rewards,
                'base_proposer_reward_gwei': base_proposer_reward_gwei,
                'total_consensus_gwei': total_consensus_gwei,
                'data_source': 'enhanced_local_calculation'
            }
            
            return int(total_consensus_gwei * 1e9), details
                
        except Exception as e:
            logging.error("Error in enhanced consensus rewards calculation: %s", str(e))
            
            # Final fallback
            estimated_consensus_gwei = 18000
            details = {
                'total_consensus_gwei': estimated_consensus_gwei,
                'data_source': 'fallback_estimate',
                'error': str(e)
            }
            return estimated_consensus_gwei * 1e9, details

    def _get_execution_rewards_beaconchain(self, slot: int, block_number: int) -> Tuple[int, int, dict]:
        """
        Get execution and MEV rewards from beaconcha.in API.
        Returns: (execution_wei, mev_wei, details)
        """
        if not self.enable_external_apis:
            return 0, 0, {}
            
        try:
            self._rate_limit_wait("beaconchain")
            
            response = requests.get(
                f"{BEACONCHAIN_API_BASE}/execution/block/{block_number}",
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') == 'OK' and data.get('data') and len(data['data']) > 0:
                    block_data = data['data'][0]
                    
                    if block_data is None:
                        return 0, 0, {'data_source': 'beaconchain_null_data'}
                    
                    # Extract reward components
                    block_reward_wei = int(block_data.get('blockReward', 0))
                    mev_reward_wei = int(block_data.get('blockMevReward', 0))
                    producer_reward_wei = int(block_data.get('producerReward', 0))
                    
                    # Check if it's a MEV-Boost block
                    relay_info = block_data.get('relay', {}) or {}
                    is_mev_boost = bool(relay_info.get('tag'))
                    
                    # What proposer actually received from execution layer
                    total_execution_reward = producer_reward_wei
                    mev_breakdown = mev_reward_wei if is_mev_boost else 0
                    traditional_fees = max(0, total_execution_reward - mev_breakdown)
                    
                    # Get additional useful data
                    gas_used = block_data.get('gasUsed', 0)
                    gas_limit = block_data.get('gasLimit', 0)
                    base_fee = block_data.get('baseFee', 0)
                    tx_count = block_data.get('txCount', 0)
                    fee_recipient = block_data.get('feeRecipient', '')
                    
                    details = {
                        'block_reward_wei': block_reward_wei,
                        'producer_reward_wei': producer_reward_wei,
                        'execution_reward_wei': total_execution_reward,
                        'mev_breakdown_wei': mev_breakdown,
                        'traditional_fees_wei': traditional_fees,
                        'gas_used': gas_used,
                        'gas_limit': gas_limit,
                        'gas_utilization': (gas_used / gas_limit * 100) if gas_limit > 0 else 0,
                        'base_fee_per_gas': base_fee,
                        'transaction_count': tx_count,
                        'fee_recipient': fee_recipient,
                        'is_mev_boost_block': is_mev_boost,
                        'relay_tag': relay_info.get('tag', '') if is_mev_boost else '',
                        'builder_pubkey': relay_info.get('builderPubkey', '') if is_mev_boost else '',
                        'data_source': 'beaconchain_api'
                    }
                    
                    return total_execution_reward, mev_breakdown, details
                    
                else:
                    return 0, 0, {'data_source': 'beaconchain_no_data'}
                    
            elif response.status_code == 404:
                return 0, 0, {'data_source': 'beaconchain_not_found'}
            else:
                return 0, 0, {'data_source': 'beaconchain_error', 'status_code': response.status_code}
                
        except requests.exceptions.Timeout:
            return 0, 0, {'data_source': 'beaconchain_timeout'}
        except Exception as e:
            return 0, 0, {'data_source': 'beaconchain_error', 'error': str(e)[:100]}

    def _get_execution_rewards_local(self, execution_payload: dict) -> Tuple[int, dict]:
        """Calculate execution rewards from local transaction analysis (fallback only)."""
        try:
            base_fee_per_gas = int(execution_payload.get('base_fee_per_gas', '0'))
            gas_used = int(execution_payload.get('gas_used', '0'))
            gas_limit = int(execution_payload.get('gas_limit', '0'))
            transactions = execution_payload.get('transactions', [])
            
            total_priority_fees = 0
            
            # Try to get recent execution block if available
            block_number = int(execution_payload.get('block_number', '0'))
            current_block = self.web3.eth.block_number
            
            if current_block - block_number < 100:
                try:
                    execution_block = self.web3.eth.get_block(block_number, full_transactions=True)
                    
                    for tx in execution_block.transactions:
                        if hasattr(tx, 'maxPriorityFeePerGas') and hasattr(tx, 'maxFeePerGas'):
                            priority_fee = min(
                                tx.maxPriorityFeePerGas,
                                tx.maxFeePerGas - base_fee_per_gas
                            )
                            if priority_fee > 0:
                                receipt = self.web3.eth.get_transaction_receipt(tx.hash)
                                total_priority_fees += priority_fee * receipt.gasUsed
                                
                except Exception as e:
                    logging.debug("Error getting execution block details: %s", str(e))
            
            # Fallback: estimate from gas data
            if total_priority_fees == 0 and gas_used > 0:
                avg_priority_fee = 2 * 1e9  # 2 gwei estimate
                total_priority_fees = avg_priority_fee * gas_used
            
            details = {
                'base_fee_per_gas': base_fee_per_gas,
                'gas_used': gas_used,
                'gas_limit': gas_limit,
                'gas_utilization': (gas_used / gas_limit * 100) if gas_limit > 0 else 0,
                'transaction_count': len(transactions),
                'priority_fees_wei': total_priority_fees,
                'data_source': 'local_execution_analysis'
            }
            
            return total_priority_fees, details
            
        except Exception as e:
            logging.debug("Error calculating local execution rewards: %s", str(e))
            return 0, {}

    def _detect_mev_heuristic(self, execution_payload: dict) -> Tuple[int, dict]:
        """Fallback MEV detection using heuristics."""
        try:
            gas_used = int(execution_payload.get('gas_used', '0'))
            gas_limit = int(execution_payload.get('gas_limit', '0'))
            transaction_count = len(execution_payload.get('transactions', []))
            
            mev_score = 0
            estimated_mev = 0
            
            # Scoring heuristics
            if gas_used > (gas_limit * 0.95):
                mev_score += 3
                estimated_mev += 0.02 * 1e18
                
            if transaction_count > 200:
                mev_score += 2
                estimated_mev += 0.01 * 1e18
                
            if gas_used > 25000000:
                mev_score += 2
                estimated_mev += 0.015 * 1e18
                
            details = {
                'mev_score': mev_score,
                'estimated_mev_wei': estimated_mev,
                'gas_used': gas_used,
                'gas_utilization': (gas_used / gas_limit * 100) if gas_limit > 0 else 0,
                'transaction_count': transaction_count,
                'data_source': 'heuristic_analysis'
            }
            
            return int(estimated_mev), details
            
        except Exception as e:
            logging.debug("Error in MEV heuristic detection: %s", str(e))
            return 0, {}

    def _calculate_rewards(self, beacon_block: dict) -> Tuple[RewardComponents, dict]:
        """
        Calculate rewards using:
        1. Local Lighthouse for consensus rewards (archive mode)
        2. Beaconcha.in for execution and MEV rewards
        3. Graffiti analysis for consensus client identification
        """
        slot = int(beacon_block['message']['slot'])
        proposer_index = int(beacon_block['message']['proposer_index'])
        execution_payload = beacon_block['message']['body']['execution_payload']
        fee_recipient = Web3.to_checksum_address(execution_payload['fee_recipient'])
        block_number = int(execution_payload['block_number'])
        block_hash = execution_payload.get('block_hash', '')
        
        # Extract graffiti data
        graffiti_data = self._extract_graffiti_data(beacon_block)
        
        # 1. CONSENSUS REWARDS - Use local Lighthouse
        consensus_wei, consensus_details = self._get_consensus_rewards_enhanced_local(slot, proposer_index, beacon_block)
        
        # 2. EXECUTION + MEV REWARDS - Use beaconcha.in API
        execution_wei, mev_breakdown_wei, beaconchain_details = self._get_execution_rewards_beaconchain(slot, block_number)
        
        # Fallback to local calculation if beaconcha.in fails
        if execution_wei == 0 and beaconchain_details.get('data_source') != 'beaconchain_api':
            execution_local_wei, execution_local_details = self._get_execution_rewards_local(execution_payload)
            mev_local_wei, mev_local_details = self._detect_mev_heuristic(execution_payload)
            
            # In local fallback, execution = traditional fees + MEV
            execution_wei = execution_local_wei + mev_local_wei
            mev_breakdown_wei = mev_local_wei
            
            # Combine fallback details
            combined_details = {
                **execution_local_details,
                **{f"mev_{k}": v for k, v in mev_local_details.items()},
                'execution_reward_wei': execution_wei,
                'mev_breakdown_wei': mev_breakdown_wei,
                'traditional_fees_wei': execution_local_wei,
                'fallback_reason': 'beaconchain_api_failed'
            }
            
        else:
            # Use beaconcha.in data
            combined_details = beaconchain_details
        
        # Create reward components
        rewards = RewardComponents(
            consensus_wei=consensus_wei,
            execution_wei=execution_wei,
            mev_wei=mev_breakdown_wei,
            data_sources=[
                consensus_details.get('data_source', 'unknown'),
                combined_details.get('data_source', 'unknown')
            ]
        )
        
        # Combine all details for clean output format
        all_details = {
            'slot': slot,
            'block_number': block_number,
            'proposer_index': proposer_index,
            'fee_recipient': fee_recipient,
            
            # Graffiti and consensus client data
            **graffiti_data,
            
            # Clean format matching original with full decimal precision
            'consensus_reward_eth': round(consensus_wei / 1e18, 10),
            'execution_fees_eth': round(execution_wei / 1e18, 10),
            'mev_breakdown_eth': round(mev_breakdown_wei / 1e18, 10),
            'mev_percentage': round((mev_breakdown_wei / execution_wei * 100), 2) if execution_wei > 0 else 0,
            
            # Gas and transaction details (original format)
            'gas_used': combined_details.get('gas_used', 0),
            'gas_limit': combined_details.get('gas_limit', 0),
            'base_fee': combined_details.get('base_fee_per_gas', 0),
            'tx_count': combined_details.get('transaction_count', 0),
            'gas_utilization': round(combined_details.get('gas_utilization', 0), 2),
            
            # MEV-specific fields (new)
            'is_mev_boost_block': combined_details.get('is_mev_boost_block', False),
            'relay_tag': combined_details.get('relay_tag', ''),
            'builder_pubkey': combined_details.get('builder_pubkey', ''),
            
            # Overall totals
            'total_rewards_wei': rewards.total_wei,
            'total_rewards_eth': round(rewards.total_wei / 1e18, 10),
            
            # Data source tracking
            'data_sources_used': rewards.data_sources,
            'calculation_method': 'lighthouse_plus_beaconchain_plus_graffiti',
            
            # Detailed breakdowns (for advanced analysis, but not cluttering main format)
            'detailed_consensus': {
                'consensus_rewards_wei': consensus_wei,
                **{k: v for k, v in consensus_details.items() if k.startswith(('attestation', 'sync', 'deposit', 'slashing', 'base_proposer'))}
            },
            'detailed_execution': {
                'execution_reward_wei': execution_wei,
                'mev_breakdown_wei': mev_breakdown_wei,
                'traditional_fees_wei': combined_details.get('traditional_fees_wei', 0),
                **{k: v for k, v in combined_details.items() if k.startswith(('block_reward', 'producer_reward'))}
            }
        }
        
        return rewards, all_details

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

    def _analyze_client_diversity(self, proposals: List[dict]) -> dict:
        """Analyze consensus client diversity from proposals."""
        client_stats = defaultdict(lambda: {
            'proposal_count': 0,
            'operators': set(),
            'total_value_eth': 0,
            'versions': set()
        })
        
        total_proposals = 0
        identified_proposals = 0
        
        for proposal in proposals:
            total_proposals += 1
            client = proposal.get('consensus_client')
            operator = proposal.get('operator')
            value = proposal.get('total_value_eth', 0)
            version = proposal.get('client_version')
            
            if client:
                identified_proposals += 1
                client_stats[client]['proposal_count'] += 1
                client_stats[client]['operators'].add(operator)
                client_stats[client]['total_value_eth'] += value
                if version:
                    client_stats[client]['versions'].add(version)
        
        # Convert sets to lists for JSON serialization and calculate percentages
        diversity_stats = {}
        for client, stats in client_stats.items():
            diversity_stats[client] = {
                'proposal_count': stats['proposal_count'],
                'proposal_percentage': (stats['proposal_count'] / identified_proposals * 100) if identified_proposals > 0 else 0,
                'unique_operators': len(stats['operators']),
                'total_value_eth': stats['total_value_eth'],
                'value_percentage': (stats['total_value_eth'] / sum(s['total_value_eth'] for s in client_stats.values()) * 100) if sum(s['total_value_eth'] for s in client_stats.values()) > 0 else 0,
                'versions_detected': list(stats['versions'])
            }
        
        return {
            'total_proposals': total_proposals,
            'identified_proposals': identified_proposals,
            'identification_rate': (identified_proposals / total_proposals * 100) if total_proposals > 0 else 0,
            'client_distribution': diversity_stats,
            'analysis_timestamp': datetime.datetime.now().isoformat()
        }

    def _save_proposals(self, proposals: List[dict]) -> None:
        """Save comprehensive proposal data to file."""
        try:
            operator_stats = defaultdict(lambda: {
                'count': 0, 
                'total_value': 0,
                'consensus_rewards': 0,
                'execution_rewards': 0,
                'mev_rewards': 0,
                'mev_blocks': 0,
                'clients_used': defaultdict(int),
                'pool_signatures': 0
            })
            
            for proposal in proposals:
                op = proposal['operator']
                operator_stats[op]['count'] += 1
                operator_stats[op]['total_value'] += proposal['total_value_eth']
                operator_stats[op]['consensus_rewards'] += proposal.get('consensus_reward_eth', 0)
                operator_stats[op]['execution_rewards'] += proposal.get('execution_fees_eth', 0)
                operator_stats[op]['mev_rewards'] += proposal.get('mev_breakdown_eth', 0)
                if proposal.get('is_mev_boost_block', False):
                    operator_stats[op]['mev_blocks'] += 1
                
                # Track consensus clients used
                client = proposal.get('consensus_client')
                if client:
                    operator_stats[op]['clients_used'][client] += 1
                
                # Track pool signatures
                if proposal.get('has_pool_signature', False):
                    operator_stats[op]['pool_signatures'] += 1

            # Calculate aggregate statistics
            total_proposals = len(proposals)
            total_value = sum(p['total_value_eth'] for p in proposals)
            total_consensus = sum(p.get('consensus_reward_eth', 0) for p in proposals)
            total_execution = sum(p.get('execution_fees_eth', 0) for p in proposals)
            total_mev = sum(p.get('mev_breakdown_eth', 0) for p in proposals)
            mev_boost_count = len([p for p in proposals if p.get('is_mev_boost_block', False)])
            
            # Analyze client diversity
            client_diversity = self._analyze_client_diversity(proposals)

            data = {
                'metadata': {
                    'last_updated': datetime.datetime.now().isoformat(),
                    'total_proposals': total_proposals,
                    'total_value_eth': total_value,
                    'total_consensus_eth': total_consensus,
                    'total_execution_eth': total_execution,
                    'total_mev_eth': total_mev,
                    'mev_boost_blocks': mev_boost_count,
                    'mev_boost_percentage': (mev_boost_count / total_proposals * 100) if total_proposals > 0 else 0,
                    'operators_tracked': len(operator_stats),
                    'data_sources': ['local_lighthouse', 'beaconchain_api', 'graffiti_analysis'],
                    'calculation_method': 'lighthouse_plus_beaconchain_plus_graffiti'
                },
                'client_diversity': client_diversity,
                'operator_summary': {
                    op: {
                        'proposal_count': stats['count'],
                        'total_value_eth': stats['total_value'],
                        'average_value_eth': stats['total_value'] / stats['count'],
                        'consensus_rewards_eth': stats['consensus_rewards'],
                        'execution_rewards_eth': stats['execution_rewards'],
                        'mev_rewards_eth': stats['mev_rewards'],
                        'mev_blocks_count': stats['mev_blocks'],
                        'mev_blocks_percentage': (stats['mev_blocks'] / stats['count'] * 100) if stats['count'] > 0 else 0,
                        'clients_used': dict(stats['clients_used']),
                        'primary_client': max(stats['clients_used'].items(), key=lambda x: x[1])[0] if stats['clients_used'] else None,
                        'pool_signatures_count': stats['pool_signatures'],
                        'pool_signatures_percentage': (stats['pool_signatures'] / stats['count'] * 100) if stats['count'] > 0 else 0
                    }
                    for op, stats in operator_stats.items()
                },
                'proposals': proposals
            }

            with open(PROPOSAL_DATA_FILE, 'w') as f:
                json.dump(data, f, indent=2, cls=DecimalEncoder)

            # Update cache with diversity stats
            self.cache['client_diversity_stats'] = client_diversity

            logging.info("Saved %d comprehensive proposals totaling %.6f ETH", len(proposals), total_value)

        except Exception as e:
            logging.error("Error saving proposals: %s", str(e))

    def scan_proposals(self, max_slots: Optional[int] = None) -> int:
        """Scan for new block proposals with comprehensive reward tracking and graffiti analysis."""
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
        
        start_time = self._slot_to_timestamp(start_slot)
        end_time = self._slot_to_timestamp(end_slot)
        start_date = datetime.datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S")
        end_date = datetime.datetime.fromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"=== COMPREHENSIVE NODESET PROPOSAL SCAN WITH GRAFFITI ANALYSIS ===")
        print(f"Time range: {start_date} to {end_date}")
        print(f"Slot range: {start_slot:,} to {end_slot:,} ({total_slots:,} slots)")
        print(f"Tracking {len(self.tracked_validators)} validators")
        print(f"External APIs enabled: {self.enable_external_apis}")
        print(f"Data sources: Local Lighthouse + {'Beaconcha.in' if self.enable_external_apis else 'Local only'} + Graffiti Analysis")
        
        if self.cache['last_slot'] > 0:
            print(f"Resuming from slot {self.cache['last_slot']:,}")
            print(f"Previous proposals found: {self.cache['proposals_found']}")

        proposals_found = 0
        slots_processed = 0
        api_calls = 0
        skipped_slots = 0
        client_detections = 0
        existing_proposals = self._load_existing_proposals()

        chunk_size = 5000  # Smaller chunks due to more detailed processing
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
                        
                        print(f"  ✓ Found proposal: slot {slot:,}, validator {proposer_index}")
                        
                        # Calculate comprehensive rewards including graffiti
                        rewards, details = self._calculate_rewards(block_data)
                        
                        epoch = slot // SLOTS_PER_EPOCH
                        timestamp = self._slot_to_timestamp(slot)
                        date_str = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
                        
                        proposal = {
                            'slot': slot,
                            'epoch': epoch,
                            'timestamp': timestamp,
                            'date': date_str,
                            'validator_index': proposer_index,
                            'validator_pubkey': validator_info['pubkey'],
                            'operator': validator_info['operator'],
                            'operator_name': self._format_operator_name(validator_info),
                            'total_value_wei': rewards.total_wei,
                            'total_value_eth': round(rewards.total_wei / 1e18, 10),
                            **details
                        }

                        existing_proposals.append(proposal)
                        proposals_found += 1

                        # Track client detection
                        if details.get('consensus_client'):
                            client_detections += 1

                        operator_display = self._format_operator_name(validator_info)
                        consensus_eth = details.get('consensus_reward_eth', 0)
                        execution_eth = details.get('execution_fees_eth', 0)
                        mev_eth = details.get('mev_breakdown_eth', 0)
                        is_mev = details.get('is_mev_boost_block', False)
                        client = details.get('consensus_client')
                        version = details.get('client_version')
                        
                        print(f"    {operator_display}")
                        print(f"    Consensus: {consensus_eth:.6f} ETH, Execution: {execution_eth:.6f} ETH")
                        if is_mev:
                            print(f"    MEV: {mev_eth:.6f} ETH ({details.get('mev_percentage', 0):.1f}% of execution)")
                            print(f"    Relay: {details.get('relay_tag', 'unknown')}")
                        if client:
                            client_display = f"{client.title()}"
                            if version:
                                client_display += f" v{version}"
                            print(f"    Client: {client_display}")
                        print(f"    Total: {rewards.total_wei/1e18:.6f} ETH")
                        
                        logging.info("Proposal: slot %d, validator %d, total %.6f ETH (cons: %.6f, exec: %.6f, mev: %.6f, client: %s)", 
                                   slot, proposer_index, rewards.total_wei/1e18, consensus_eth, execution_eth, mev_eth, client or 'unknown')

                    if slots_processed % 10000 == 0:
                        progress_pct = slots_processed / total_slots * 100
                        print(f"  Progress: {slots_processed:,}/{total_slots:,} slots ({progress_pct:.1f}%), {proposals_found} proposals, {client_detections} clients detected, {api_calls:,} API calls")
                        
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
            chunk_clients = len([p for p in existing_proposals if chunk_start <= p['slot'] <= chunk_end and p.get('consensus_client')])
            print(f"Chunk complete: {chunk_proposals} proposals found, {chunk_clients} clients identified in slots {chunk_start:,}-{chunk_end:,}")

        self.cache.update({
            'last_slot': end_slot,
            'proposals_found': self.cache['proposals_found'] + proposals_found
        })

        self._save_cache()
        if existing_proposals:
            self._save_proposals(existing_proposals)

        print(f"\n=== COMPREHENSIVE SCAN WITH GRAFFITI ANALYSIS COMPLETE ===")
        print(f"  Slots processed: {slots_processed:,}")
        print(f"  Skipped slots (no block): {skipped_slots:,}")
        print(f"  Proposals found: {proposals_found}")
        print(f"  Consensus clients identified: {client_detections}/{proposals_found} ({client_detections/proposals_found*100:.1f}%)" if proposals_found > 0 else "  No proposals found")
        print(f"  API calls made: {api_calls:,}")
        print(f"  Total proposals tracked: {len(existing_proposals)}")
        
        if proposals_found > 0:
            total_value = sum(p['total_value_eth'] for p in existing_proposals if p['slot'] >= start_slot)
            mev_blocks = len([p for p in existing_proposals if p['slot'] >= start_slot and p.get('is_mev_boost_block', False)])
            print(f"  Total value (new): {total_value:.6f} ETH")
            print(f"  MEV-Boost blocks: {mev_blocks}/{proposals_found} ({mev_blocks/proposals_found*100:.1f}%)")
        
        return proposals_found

    def generate_comprehensive_report(self) -> None:
        """Generate comprehensive proposal summary report with client diversity analysis."""
        if not os.path.exists(PROPOSAL_DATA_FILE):
            print("No proposal data available")
            return

        try:
            with open(PROPOSAL_DATA_FILE, 'r') as f:
                data = json.load(f)

            metadata = data.get('metadata', {})
            operator_summary = data.get('operator_summary', {})
            proposals = data.get('proposals', [])
            client_diversity = data.get('client_diversity', {})

            print("\n" + "="*80)
            print("COMPREHENSIVE NODESET BLOCK PROPOSAL REPORT WITH CLIENT DIVERSITY")
            print("="*80)

            total_proposals = metadata.get('total_proposals', 0)
            total_value = metadata.get('total_value_eth', 0)
            total_consensus = metadata.get('total_consensus_eth', 0)
            total_execution = metadata.get('total_execution_eth', 0)
            total_mev = metadata.get('total_mev_eth', 0)
            mev_boost_blocks = metadata.get('mev_boost_blocks', 0)
            mev_boost_pct = metadata.get('mev_boost_percentage', 0)
            operators_count = metadata.get('operators_tracked', 0)

            print(f"\n=== OVERALL STATISTICS ===")
            print(f"Total proposals: {total_proposals:,}")
            print(f"Total value: {total_value:.6f} ETH")
            print(f"  Consensus rewards: {total_consensus:.6f} ETH ({total_consensus/total_value*100:.1f}%)")
            print(f"  Execution rewards: {total_execution:.6f} ETH ({total_execution/total_value*100:.1f}%)")
            print(f"  MEV component: {total_mev:.6f} ETH ({total_mev/total_value*100:.1f}%)")
            print(f"MEV-Boost blocks: {mev_boost_blocks:,}/{total_proposals:,} ({mev_boost_pct:.1f}%)")
            print(f"Operators with proposals: {operators_count}")

            # Client diversity analysis
            if client_diversity:
                print(f"\n=== CONSENSUS CLIENT DIVERSITY ===")
                identified_proposals = client_diversity.get('identified_proposals', 0)
                identification_rate = client_diversity.get('identification_rate', 0)
                
                print(f"Total proposals analyzed: {total_proposals:,}")
                print(f"Proposals with client identification: {identified_proposals:,} ({identification_rate:.1f}%)")
                
                client_distribution = client_diversity.get('client_distribution', {})
                if client_distribution:
                    print("\nClient distribution by proposals:")
                    for client, stats in sorted(client_distribution.items(), key=lambda x: x[1]['proposal_count'], reverse=True):
                        count = stats['proposal_count']
                        percentage = stats['proposal_percentage']
                        operators = stats['unique_operators']
                        value_eth = stats['total_value_eth']
                        value_pct = stats['value_percentage']
                        
                        print(f"  {client.title()}: {count} proposals ({percentage:.1f}%), {operators} operators, {value_eth:.6f} ETH ({value_pct:.1f}%)")
                        
                        versions = stats.get('versions_detected', [])
                        if versions:
                            print(f"    Versions: {', '.join(sorted(versions))}")

            if operator_summary:
                print(f"\n=== TOP OPERATORS BY PROPOSALS ===")
                sorted_operators = sorted(
                    operator_summary.items(),
                    key=lambda x: x[1]['proposal_count'],
                    reverse=True
                )

                for i, (operator, stats) in enumerate(sorted_operators[:15]):
                    ens_name = self.validator_data.get('ens_names', {}).get(operator)
                    display_name = f"{ens_name} ({operator[:8]}...)" if ens_name else f"{operator[:8]}..."
                    
                    count = stats['proposal_count']
                    value = stats['total_value_eth']
                    avg = stats['average_value_eth']
                    consensus = stats['consensus_rewards_eth']
                    execution = stats['execution_rewards_eth']
                    mev = stats['mev_rewards_eth']
                    mev_blocks = stats['mev_blocks_count']
                    mev_pct = stats['mev_blocks_percentage']
                    primary_client = stats.get('primary_client')
                    clients_used = stats.get('clients_used', {})
                    pool_sigs = stats.get('pool_signatures_count', 0)

                    print(f"{count:3d} proposals: {display_name}")
                    print(f"     Total: {value:.6f} ETH (avg: {avg:.6f} ETH)")
                    print(f"     Consensus: {consensus:.6f} ETH, Execution: {execution:.6f} ETH, MEV: {mev:.6f} ETH")
                    print(f"     MEV-Boost: {mev_blocks}/{count} blocks ({mev_pct:.1f}%)")
                    
                    if primary_client:
                        client_display = primary_client.title()
                        if len(clients_used) > 1:
                            other_clients = [c for c in clients_used.keys() if c != primary_client]
                            client_display += f" (primary), also uses: {', '.join(c.title() for c in other_clients)}"
                        print(f"     Client: {client_display}")
                    
                    if pool_sigs > 0:
                        pool_pct = stats.get('pool_signatures_percentage', 0)
                        print(f"     Pool signatures: {pool_sigs}/{count} blocks ({pool_pct:.1f}%)")

                print(f"\n=== OPERATORS BY CLIENT TYPE ===")
                # Group operators by their primary client
                client_operators = defaultdict(list)
                for operator, stats in operator_summary.items():
                    primary_client = stats.get('primary_client')
                    if primary_client:
                        proposal_count = stats['proposal_count']
                        total_value = stats['total_value_eth']
                        client_operators[primary_client].append((operator, proposal_count, total_value))
                
                for client in sorted(client_operators.keys()):
                    operators = sorted(client_operators[client], key=lambda x: x[1], reverse=True)
                    total_proposals_for_client = sum(count for _, count, _ in operators)
                    total_value_for_client = sum(value for _, _, value in operators)
                    
                    print(f"\n{client.title()} operators ({len(operators)} operators, {total_proposals_for_client} proposals, {total_value_for_client:.6f} ETH):")
                    for operator, count, value in operators[:5]:  # Top 5 for each client
                        ens_name = self.validator_data.get('ens_names', {}).get(operator)
                        display_name = f"{ens_name} ({operator[:8]}...)" if ens_name else f"{operator[:8]}..."
                        print(f"  {count} proposals ({value:.6f} ETH): {display_name}")

                print(f"\n=== TOP OPERATORS BY VALUE ===")
                sorted_by_value = sorted(
                    operator_summary.items(),
                    key=lambda x: x[1]['total_value_eth'],
                    reverse=True
                )

                for i, (operator, stats) in enumerate(sorted_by_value[:10]):
                    ens_name = self.validator_data.get('ens_names', {}).get(operator)
                    display_name = f"{ens_name} ({operator[:8]}...)" if ens_name else f"{operator[:8]}..."
                    
                    value = stats['total_value_eth']
                    count = stats['proposal_count']
                    mev = stats['mev_rewards_eth']
                    client = stats.get('primary_client', 'Unknown')

                    print(f"{value:.6f} ETH: {display_name} ({count} proposals, {mev:.6f} ETH MEV, {client.title()})")

            if proposals:
                print(f"\n=== RECENT HIGH-VALUE PROPOSALS ===")
                recent_high_value = sorted(
                    [p for p in proposals if p['total_value_eth'] > 0.01], 
                    key=lambda x: x['total_value_eth'], 
                    reverse=True
                )[:10]

                for proposal in recent_high_value:
                    slot = proposal['slot']
                    date = proposal['date']
                    operator_name = proposal['operator_name']
                    value = proposal['total_value_eth']
                    consensus = proposal.get('consensus_reward_eth', 0)
                    execution = proposal.get('execution_fees_eth', 0)
                    mev = proposal.get('mev_breakdown_eth', 0)
                    is_mev = proposal.get('is_mev_boost_block', False)
                    relay = proposal.get('relay_tag', '')
                    client = proposal.get('consensus_client', 'Unknown')
                    version = proposal.get('client_version', '')

                    print(f"Slot {slot:,} ({date}): {operator_name}")
                    print(f"  Value: {value:.6f} ETH (C: {consensus:.6f}, E: {execution:.6f}, MEV: {mev:.6f})")
                    if is_mev and relay:
                        print(f"  MEV-Boost via {relay}")
                    client_display = client.title()
                    if version:
                        client_display += f" v{version}"
                    print(f"  Client: {client_display}")

        except Exception as e:
            logging.error("Error generating comprehensive report: %s", str(e))


def main():
    """Main execution function."""
    eth_client_url = os.getenv('ETH_CLIENT_URL')
    beacon_api_url = os.getenv('BEACON_API_URL')

    if not eth_client_url:
        raise ValueError("ETH_CLIENT_URL environment variable is required")
    if not beacon_api_url:
        raise ValueError("BEACON_API_URL environment variable is required")

    print("Comprehensive NodeSet Block Proposal Tracker with Graffiti Analysis")
    print("Consensus rewards: Local archive Lighthouse (beacon chain duties)")
    print("Execution rewards: Beaconcha.in API (total execution layer rewards)")
    print("MEV breakdown: Tracked separately within execution rewards")
    print("Client identification: Graffiti analysis from beacon blocks")
    print("Fallback: Local analysis if external APIs fail")

    tracker = ComprehensiveProposalTracker(eth_client_url, beacon_api_url, enable_external_apis=True)
    proposals_found = tracker.scan_proposals()
    tracker.generate_comprehensive_report()


if __name__ == "__main__":
    main()
