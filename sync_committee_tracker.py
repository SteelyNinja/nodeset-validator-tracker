"""
NodeSet Sync Committee Participation Tracker
Tracks sync committee participation for NodeSet validators

Data Sources:
1. Local: Lighthouse beacon API for all sync committee data
2. Tracks successful and missed sync committee attestations
3. Handles partial committee periods during scanning
"""

import os
import json
import time
import logging
import requests
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Set, Optional
import datetime
from dataclasses import dataclass

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder to avoid scientific notation for small numbers."""
    def encode(self, obj):
        if isinstance(obj, float):
            return format(obj, 'f')
        return super(DecimalEncoder, self).encode(obj)
    
    def iterencode(self, obj, _one_shot=False):
        if isinstance(obj, float):
            yield format(obj, 'f')
        else:
            for chunk in super(DecimalEncoder, self).iterencode(obj, _one_shot):
                yield chunk

# Configuration
logging.basicConfig(
    level=logging.INFO,
    filename='sync_committee_tracker.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
GENESIS_TIME = 1606824023
SECONDS_PER_SLOT = 12
SLOTS_PER_EPOCH = 32
EPOCHS_PER_SYNC_COMMITTEE_PERIOD = 256
SYNC_COMMITTEE_SIZE = 512

# Cache files
VALIDATOR_CACHE_FILE = "nodeset_validator_tracker_cache.json"
SYNC_COMMITTEE_CACHE_FILE = "sync_committee_cache.json"
SYNC_COMMITTEE_DATA_FILE = "sync_committee_participation.json"

@dataclass
class SyncCommitteeStats:
    """Structure for sync committee participation statistics"""
    period: int
    start_epoch: int
    end_epoch: int
    start_slot: int
    end_slot: int
    validator_index: int
    operator: str
    operator_name: str
    total_slots: int = 0
    successful_attestations: int = 0
    missed_attestations: int = 0
    participation_rate: float = 0.0
    is_partial_period: bool = False
    scan_start_slot: Optional[int] = None
    scan_end_slot: Optional[int] = None

class NodeSetSyncCommitteeTracker:
    """
    Comprehensive sync committee participation tracker for NodeSet validators.
    """

    def __init__(self, beacon_api_url: str):
        self.beacon_api_url = self._setup_beacon_api(beacon_api_url)
        
        self.cache = self._load_cache()
        self.validator_data = self._load_validator_data()
        self.tracked_validators = self._get_tracked_validators()
        
        # Track sync committee periods and assignments
        self.sync_committee_assignments = {}
        self.period_progress = {}

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

    def _load_cache(self) -> dict:
        """Load sync committee tracking cache."""
        if os.path.exists(SYNC_COMMITTEE_CACHE_FILE):
            try:
                with open(SYNC_COMMITTEE_CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    logging.info("Loaded sync committee cache: last slot %d", cache.get('last_slot', 0))
                    return cache
            except Exception as e:
                logging.warning("Error loading sync committee cache: %s", str(e))

        return {
            'last_slot': self._get_deployment_slot(),
            'committees_tracked': 0,
            'last_updated': 0,
            'period_assignments': {},
            'period_progress': {}
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

        logging.info("Tracking %d active validators for sync committees", len(tracked))
        return tracked

    def _save_cache(self) -> None:
        """Save sync committee tracking cache."""
        try:
            self.cache['last_updated'] = int(time.time())
            self.cache['period_assignments'] = self.sync_committee_assignments
            self.cache['period_progress'] = self.period_progress
            
            with open(SYNC_COMMITTEE_CACHE_FILE, 'w') as f:
                json.dump(self.cache, f, indent=2, cls=DecimalEncoder)
            logging.info("Cache saved: %d committees tracked", self.cache['committees_tracked'])
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

    def _slot_to_epoch(self, slot: int) -> int:
        """Convert slot to epoch."""
        return slot // SLOTS_PER_EPOCH

    def _epoch_to_sync_committee_period(self, epoch: int) -> int:
        """Convert epoch to sync committee period."""
        return epoch // EPOCHS_PER_SYNC_COMMITTEE_PERIOD

    def _sync_committee_period_bounds(self, period: int) -> Tuple[int, int, int, int]:
        """Get start/end epoch and slot for sync committee period."""
        start_epoch = period * EPOCHS_PER_SYNC_COMMITTEE_PERIOD
        end_epoch = start_epoch + EPOCHS_PER_SYNC_COMMITTEE_PERIOD - 1
        start_slot = start_epoch * SLOTS_PER_EPOCH
        end_slot = (end_epoch + 1) * SLOTS_PER_EPOCH - 1
        return start_epoch, end_epoch, start_slot, end_slot

    def _slot_to_timestamp(self, slot: int) -> int:
        """Convert slot to timestamp."""
        return GENESIS_TIME + (slot * SECONDS_PER_SLOT)

    def _format_operator_name(self, validator_info: dict) -> str:
        """Format operator display name."""
        ens_name = validator_info.get('ens_name')
        operator = validator_info['operator']
        
        if ens_name:
            return f"{ens_name} ({operator[:8]}...{operator[-6:]})"
        else:
            return f"{operator[:8]}...{operator[-6:]}"

    def _get_sync_committee_for_period(self, period: int) -> Optional[Dict]:
        """Get sync committee composition for a given period."""
        try:
            # Use the first slot of the period to get the sync committee
            start_epoch = period * EPOCHS_PER_SYNC_COMMITTEE_PERIOD
            start_slot = start_epoch * SLOTS_PER_EPOCH
            
            print(f"    Fetching sync committee for period {period} (epoch {start_epoch}, slot {start_slot})")
            
            # Try using slot first, then epoch if that fails
            for state_id in [start_slot, start_epoch, f"0x{start_slot:x}"]:
                try:
                    response = requests.get(
                        f"{self.beacon_api_url}/eth/v1/beacon/states/{state_id}/sync_committees",
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        data = response.json()['data']
                        print(f"    Successfully got sync committee data using state_id: {state_id}")
                        
                        # Get all validator indices in the sync committee
                        sync_committee_validators = data.get('validators', [])
                        aggregate_pubkey = data.get('aggregate_pubkey', '')
                        
                        print(f"    Sync committee has {len(sync_committee_validators)} validators")
                        
                        # Show some sample validator indices for debugging
                        if sync_committee_validators:
                            print(f"    Sample sync committee validator indices:")
                            for i, val_idx in enumerate(sync_committee_validators[:3]):
                                print(f"      {i}: {val_idx}")
                        
                        # Show some of our validator indices for comparison
                        our_sample_indices = list(self.tracked_validators.keys())[:3]
                        print(f"    Sample our validator indices:")
                        for i, val_idx in enumerate(our_sample_indices):
                            print(f"      {i}: {val_idx}")
                        
                        print(f"    Total our tracked validators: {len(self.tracked_validators)}")
                        
                        # Find which of our validators are in this sync committee
                        our_validator_indices = []
                        our_validator_pubkeys = []
                        
                        # Convert sync committee validators to integers if they're strings
                        sync_committee_indices = []
                        for val in sync_committee_validators:
                            try:
                                val_idx = int(val)
                                sync_committee_indices.append(val_idx)
                            except (ValueError, TypeError):
                                print(f"    Warning: Could not convert validator {val} to integer")
                        
                        # Check which of our validators are in the sync committee
                        for val_idx in sync_committee_indices:
                            if val_idx in self.tracked_validators:
                                our_validator_indices.append(val_idx)
                                val_info = self.tracked_validators[val_idx]
                                our_validator_pubkeys.append(val_info['pubkey'])
                        
                        print(f"    Found {len(our_validator_indices)} of our validators in sync committee")
                        if our_validator_indices:
                            for val_idx in our_validator_indices:
                                val_info = self.tracked_validators[val_idx]
                                operator_name = self._format_operator_name(val_info)
                                print(f"      - Validator {val_idx}: {operator_name}")
                        
                        # Return the result immediately after successful processing
                        return {
                            'period': period,
                            'validators': sync_committee_indices,  # Store as indices
                            'validator_indices': our_validator_indices,
                            'our_validator_pubkeys': our_validator_pubkeys,
                            'aggregate_pubkey': aggregate_pubkey
                        }
                    elif response.status_code == 404:
                        print(f"    State {state_id} not found (404), trying next...")
                        continue
                    else:
                        print(f"    Failed with state_id {state_id}: HTTP {response.status_code}")
                        continue
                        
                except requests.exceptions.Timeout:
                    print(f"    Timeout with state_id {state_id}, trying next...")
                    continue
                except Exception as e:
                    print(f"    Error with state_id {state_id}: {str(e)}, trying next...")
                    continue
            
            print(f"    Failed to get sync committee for period {period} with any state_id")
            logging.warning("Failed to get sync committee for period %d with any state_id", period)
            return None
                
        except Exception as e:
            print(f"    Error getting sync committee for period {period}: {str(e)}")
            logging.error("Error getting sync committee for period %d: %s", period, str(e))
            return None

    def _get_sync_committee_assignments_in_range(self, start_slot: int, end_slot: int) -> Dict[int, Dict]:
        """Get all sync committee assignments for periods that overlap with slot range."""
        start_epoch = self._slot_to_epoch(start_slot)
        end_epoch = self._slot_to_epoch(end_slot)
        
        start_period = self._epoch_to_sync_committee_period(start_epoch)
        end_period = self._epoch_to_sync_committee_period(end_epoch)
        
        print(f"Checking sync committee periods {start_period} to {end_period}")
        print(f"  Start: Epoch {start_epoch} (Period {start_period})")
        print(f"  End: Epoch {end_epoch} (Period {end_period})")
        print(f"  Total periods to check: {end_period - start_period + 1}")
        
        # Calculate rough probability
        try:
            # Get current total validator count from beacon API
            response = requests.get(f"{self.beacon_api_url}/eth/v1/beacon/states/head/validators", timeout=10)
            if response.status_code == 200:
                validators_data = response.json()['data']
                total_validators_estimate = len(validators_data)
            else:
                total_validators_estimate = 900000  # Fallback estimate
        except:
            total_validators_estimate = 900000  # Fallback estimate
            
        our_validators_count = len(self.tracked_validators)
        expected_per_committee = (our_validators_count / total_validators_estimate) * SYNC_COMMITTEE_SIZE
        
        print(f"\nProbability analysis:")
        print(f"  Our validators: {our_validators_count:,}")
        print(f"  Total active validators: {total_validators_estimate:,}")
        print(f"  Sync committee size: {SYNC_COMMITTEE_SIZE}")
        print(f"  Expected our validators per committee: {expected_per_committee:.2f}")
        if total_validators_estimate > our_validators_count:
            prob_none = ((total_validators_estimate - our_validators_count) / total_validators_estimate) ** SYNC_COMMITTEE_SIZE
            prob_at_least_one = (1 - prob_none) * 100
            print(f"  Probability of having ≥1 validator in a committee: {prob_at_least_one:.2f}%")
        
        assignments = {}
        
        for period in range(start_period, end_period + 1):
            print(f"\n--- Checking Period {period} ---")
            
            if period not in self.sync_committee_assignments:
                committee_data = self._get_sync_committee_for_period(period)
                if committee_data:
                    self.sync_committee_assignments[period] = committee_data
                    
                    # Check if we have any validators in this committee
                    our_validators = committee_data.get('validator_indices', [])
                    
                    if our_validators:
                        print(f"✓ Period {period}: {len(our_validators)} our validators in sync committee")
                        assignments[period] = {
                            **committee_data,
                            'our_validators': our_validators
                        }
                        logging.info("Period %d sync committee: %d our validators participating", 
                                   period, len(our_validators))
                    else:
                        print(f"✗ Period {period}: No our validators in sync committee")
                else:
                    print(f"✗ Period {period}: Failed to get sync committee data")
            else:
                # Use cached data
                print(f"Using cached data for period {period}")
                committee_data = self.sync_committee_assignments[period]
                our_validators = committee_data.get('validator_indices', [])
                
                if our_validators:
                    print(f"✓ Period {period} (cached): {len(our_validators)} our validators in sync committee")
                    assignments[period] = {
                        **committee_data,
                        'our_validators': our_validators
                    }
                else:
                    print(f"✗ Period {period} (cached): No our validators in sync committee")
        
        print(f"\nFound {len(assignments)} periods with our validators")
        return assignments

    def _check_sync_committee_participation(self, slot: int, sync_committee_validators: List[int]) -> Dict[int, bool]:
        """Check which validators participated in sync committee for a given slot."""
        try:
            response = requests.get(f"{self.beacon_api_url}/eth/v2/beacon/blocks/{slot}", timeout=30)
            
            if response.status_code == 200:
                block_data = response.json()['data']
                sync_aggregate = block_data['message']['body'].get('sync_aggregate', {})
                sync_committee_bits = sync_aggregate.get('sync_committee_bits', '')
                
                participation = {}
                
                if sync_committee_bits and sync_committee_bits.startswith('0x'):
                    try:
                        # Convert hex to binary representation
                        hex_val = int(sync_committee_bits, 16)
                        binary_str = format(hex_val, f'0{SYNC_COMMITTEE_SIZE}b')
                        
                        # Check participation for each validator
                        # Note: sync_committee_validators contains the full sync committee (512 validators)
                        # We need to find the position of our validators within this committee
                        for i, committee_validator_idx in enumerate(sync_committee_validators):
                            if i < len(binary_str):
                                # Bit is 1 if validator participated (bits are in reverse order)
                                participated = binary_str[-(i+1)] == '1'
                                participation[committee_validator_idx] = participated
                            else:
                                participation[committee_validator_idx] = False
                                
                    except Exception as e:
                        logging.debug("Error parsing sync committee bits for slot %d: %s", slot, str(e))
                        # Default to all participated if we can't parse
                        for validator_index in sync_committee_validators:
                            participation[validator_index] = True
                else:
                    # No sync aggregate data, assume all participated
                    for validator_index in sync_committee_validators:
                        participation[validator_index] = True
                
                return participation
                
            elif response.status_code == 404:
                # No block for this slot
                return {}
            else:
                logging.debug("Error fetching block %d for sync committee check: HTTP %d", slot, response.status_code)
                return {}
                
        except Exception as e:
            logging.debug("Error checking sync committee participation for slot %d: %s", slot, str(e))
            return {}

    def _load_existing_data(self) -> Dict:
        """Load existing sync committee participation data."""
        if os.path.exists(SYNC_COMMITTEE_DATA_FILE):
            try:
                with open(SYNC_COMMITTEE_DATA_FILE, 'r') as f:
                    data = json.load(f)
                    logging.info("Loaded existing sync committee data")
                    return data
            except Exception as e:
                logging.warning("Error loading existing sync committee data: %s", str(e))
        
        return {
            'metadata': {
                'last_updated': '',
                'total_periods_tracked': 0,
                'total_validators_in_committees': 0,
                'total_attestations_tracked': 0,
                'total_successful_attestations': 0,
                'total_missed_attestations': 0,
                'overall_participation_rate': 0.0
            },
            'period_summary': {},
            'validator_summary': {},
            'detailed_stats': []
        }

    def _save_data(self, data: Dict) -> None:
        """Save comprehensive sync committee participation data."""
        try:
            with open(SYNC_COMMITTEE_DATA_FILE, 'w') as f:
                json.dump(data, f, indent=2, cls=DecimalEncoder)
            logging.info("Saved sync committee participation data")
        except Exception as e:
            logging.error("Error saving sync committee data: %s", str(e))

    def debug_sync_committee_detection(self, test_period: Optional[int] = None) -> None:
        """Debug sync committee detection for a specific period."""
        current_slot = self._get_current_slot()
        current_epoch = self._slot_to_epoch(current_slot)
        current_period = self._epoch_to_sync_committee_period(current_epoch)
        
        if test_period is None:
            test_period = current_period
        
        print(f"\n=== DEBUGGING SYNC COMMITTEE DETECTION ===")
        print(f"Current slot: {current_slot}")
        print(f"Current epoch: {current_epoch}")
        print(f"Current period: {current_period}")
        print(f"Testing period: {test_period}")
        
        # Check our validator data
        print(f"\nValidator data loaded:")
        print(f"  Total tracked validators: {len(self.tracked_validators)}")
        sample_validators = list(self.tracked_validators.items())[:3]
        for val_idx, val_info in sample_validators:
            print(f"  Sample validator {val_idx}: {val_info['pubkey'][:20]}...")
        
        # Test sync committee API call
        start_epoch = test_period * EPOCHS_PER_SYNC_COMMITTEE_PERIOD
        start_slot = start_epoch * SLOTS_PER_EPOCH
        
        print(f"\nTesting sync committee API for period {test_period}:")
        print(f"  Start epoch: {start_epoch}")
        print(f"  Start slot: {start_slot}")
        
        # Try the API call
        for state_id in [start_slot, start_epoch, "head"]:
            try:
                print(f"\n  Trying state_id: {state_id}")
                response = requests.get(
                    f"{self.beacon_api_url}/eth/v1/beacon/states/{state_id}/sync_committees",
                    timeout=30
                )
                
                print(f"    Response status: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    sync_data = data.get('data', {})
                    validators = sync_data.get('validators', [])
                    
                    print(f"    Success! Sync committee size: {len(validators)}")
                    print(f"    Sample validator indices:")
                    for i, val_idx in enumerate(validators[:5]):
                        print(f"      {i}: {val_idx}")
                    
                    # Check if any match our validators
                    our_validator_indices = set(self.tracked_validators.keys())
                    
                    # Convert sync committee validators to integers if needed
                    sync_committee_indices = []
                    for val in validators:
                        try:
                            val_idx = int(val)
                            sync_committee_indices.append(val_idx)
                        except (ValueError, TypeError):
                            print(f"      Warning: Could not convert {val} to integer")
                    
                    matching_indices = [idx for idx in sync_committee_indices if idx in our_validator_indices]
                    
                    print(f"    Matching our validators: {len(matching_indices)}")
                    for val_idx in matching_indices:
                        val_info = self.tracked_validators[val_idx]
                        operator_name = self._format_operator_name(val_info)
                        print(f"      Validator {val_idx}: {operator_name}")
                    
                    break
                else:
                    print(f"    Failed: {response.status_code}")
                    if response.text:
                        print(f"    Error: {response.text[:200]}")
                        
            except Exception as e:
                print(f"    Exception: {str(e)}")
                
        print(f"\n=== DEBUG COMPLETE ===")

    def scan_sync_committee_participation(self, max_slots: Optional[int] = None, debug_mode: bool = False) -> int:
        """Scan for sync committee participation across periods."""
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
        
        print(f"=== NODESET SYNC COMMITTEE PARTICIPATION SCAN ===")
        print(f"Time range: {start_date} to {end_date}")
        print(f"Slot range: {start_slot:,} to {end_slot:,} ({total_slots:,} slots)")
        print(f"Tracking {len(self.tracked_validators)} validators")
        print(f"Data source: Local Lighthouse beacon API")
        
        # Get sync committee assignments for the range
        assignments = self._get_sync_committee_assignments_in_range(start_slot, end_slot)
        
        if not assignments:
            print("No NodeSet validators in sync committees during this period")
            print("\nRunning debug check to investigate...")
            self.debug_sync_committee_detection()
            return 0
        
        print(f"Found {len(assignments)} sync committee periods with NodeSet validators")
        for period, committee in assignments.items():
            start_epoch, end_epoch, period_start_slot, period_end_slot = self._sync_committee_period_bounds(period)
            print(f"  Period {period}: Epochs {start_epoch}-{end_epoch}, {len(committee['our_validators'])} our validators")
        
        # Load existing data
        existing_data = self._load_existing_data()
        detailed_stats = existing_data.get('detailed_stats', [])
        
        # Track progress and participation
        total_attestations_checked = 0
        total_successful = 0
        total_missed = 0
        periods_processed = set()
        
        # Process each sync committee period that overlaps with our scan range
        for period, committee in assignments.items():
            start_epoch, end_epoch, period_start_slot, period_end_slot = self._sync_committee_period_bounds(period)
            
            # Determine actual scan range for this period
            scan_start = max(start_slot, period_start_slot)
            scan_end = min(end_slot, period_end_slot)
            
            is_partial_period = (scan_start > period_start_slot) or (scan_end < period_end_slot)
            
            print(f"\nProcessing Period {period} (Epochs {start_epoch}-{end_epoch})")
            print(f"  Period slots: {period_start_slot:,} to {period_end_slot:,}")
            print(f"  Scan slots: {scan_start:,} to {scan_end:,}")
            print(f"  Partial period: {is_partial_period}")
            print(f"  Our validators: {len(committee['our_validators'])}")
            
            # Initialize or load existing stats for this period
            period_stats = {}
            for val_idx in committee['our_validators']:
                validator_info = self.tracked_validators[val_idx]
                
                # Check if we already have stats for this validator in this period
                existing_stat = None
                for stat in detailed_stats:
                    if stat['period'] == period and stat['validator_index'] == val_idx:
                        existing_stat = stat
                        break
                
                if existing_stat:
                    period_stats[val_idx] = SyncCommitteeStats(
                        period=period,
                        start_epoch=start_epoch,
                        end_epoch=end_epoch,
                        start_slot=period_start_slot,
                        end_slot=period_end_slot,
                        validator_index=val_idx,
                        operator=validator_info['operator'],
                        operator_name=self._format_operator_name(validator_info),
                        total_slots=existing_stat.get('total_slots', 0),
                        successful_attestations=existing_stat.get('successful_attestations', 0),
                        missed_attestations=existing_stat.get('missed_attestations', 0),
                        is_partial_period=is_partial_period,
                        scan_start_slot=scan_start,
                        scan_end_slot=scan_end
                    )
                else:
                    period_stats[val_idx] = SyncCommitteeStats(
                        period=period,
                        start_epoch=start_epoch,
                        end_epoch=end_epoch,
                        start_slot=period_start_slot,
                        end_slot=period_end_slot,
                        validator_index=val_idx,
                        operator=validator_info['operator'],
                        operator_name=self._format_operator_name(validator_info),
                        is_partial_period=is_partial_period,
                        scan_start_slot=scan_start,
                        scan_end_slot=scan_end
                    )
            
            # Check participation for each slot in the range
            slots_processed = 0
            for slot in range(scan_start, scan_end + 1):
                # Pass the full sync committee for participation checking
                participation = self._check_sync_committee_participation(slot, committee['validators'])
                
                # Only count participation for our validators
                for val_idx in committee['our_validators']:
                    if val_idx in participation:
                        period_stats[val_idx].total_slots += 1
                        total_attestations_checked += 1
                        
                        if participation[val_idx]:
                            period_stats[val_idx].successful_attestations += 1
                            total_successful += 1
                        else:
                            period_stats[val_idx].missed_attestations += 1
                            total_missed += 1
                
                slots_processed += 1
                
                if slots_processed % 5000 == 0:
                    progress_pct = slots_processed / (scan_end - scan_start + 1) * 100
                    print(f"    Progress: {slots_processed:,}/{scan_end - scan_start + 1:,} slots ({progress_pct:.1f}%)")
            
            # Calculate participation rates and update detailed stats
            for val_idx, stats in period_stats.items():
                if stats.total_slots > 0:
                    stats.participation_rate = stats.successful_attestations / stats.total_slots * 100
                
                # Update or add to detailed stats
                existing_stat = None
                for i, stat in enumerate(detailed_stats):
                    if stat['period'] == period and stat['validator_index'] == val_idx:
                        existing_stat = i
                        break
                
                stat_dict = {
                    'period': stats.period,
                    'start_epoch': stats.start_epoch,
                    'end_epoch': stats.end_epoch,
                    'start_slot': stats.start_slot,
                    'end_slot': stats.end_slot,
                    'validator_index': stats.validator_index,
                    'validator_pubkey': self.tracked_validators[val_idx]['pubkey'],
                    'operator': stats.operator,
                    'operator_name': stats.operator_name,
                    'total_slots': stats.total_slots,
                    'successful_attestations': stats.successful_attestations,
                    'missed_attestations': stats.missed_attestations,
                    'participation_rate': round(stats.participation_rate, 2),
                    'is_partial_period': stats.is_partial_period,
                    'scan_start_slot': stats.scan_start_slot,
                    'scan_end_slot': stats.scan_end_slot
                }
                
                if existing_stat is not None:
                    detailed_stats[existing_stat] = stat_dict
                else:
                    detailed_stats.append(stat_dict)
                
                validator_info = self.tracked_validators[val_idx]
                operator_display = self._format_operator_name(validator_info)
                print(f"    {operator_display}: {stats.successful_attestations}/{stats.total_slots} ({stats.participation_rate:.1f}%)")
            
            periods_processed.add(period)
            print(f"  Period {period} complete: {len(committee['our_validators'])} validators processed")
        
        # Update cache
        self.cache.update({
            'last_slot': end_slot,
            'committees_tracked': len(periods_processed)
        })
        self._save_cache()
        
        # Calculate aggregate statistics
        operator_summary = defaultdict(lambda: {
            'total_periods': 0,
            'total_slots': 0,
            'total_successful': 0,
            'total_missed': 0,
            'participation_rate': 0.0
        })
        
        period_summary = defaultdict(lambda: {
            'our_validators_count': 0,
            'total_slots': 0,
            'total_successful': 0,
            'total_missed': 0,
            'participation_rate': 0.0
        })
        
        unique_validators = set()
        
        for stat in detailed_stats:
            operator = stat['operator']
            period = stat['period']
            
            operator_summary[operator]['total_periods'] += 1
            operator_summary[operator]['total_slots'] += stat['total_slots']
            operator_summary[operator]['total_successful'] += stat['successful_attestations']
            operator_summary[operator]['total_missed'] += stat['missed_attestations']
            
            period_summary[period]['our_validators_count'] += 1
            period_summary[period]['total_slots'] += stat['total_slots']
            period_summary[period]['total_successful'] += stat['successful_attestations']
            period_summary[period]['total_missed'] += stat['missed_attestations']
            
            unique_validators.add(stat['validator_index'])
        
        # Calculate participation rates
        for operator in operator_summary:
            total = operator_summary[operator]['total_slots']
            if total > 0:
                operator_summary[operator]['participation_rate'] = operator_summary[operator]['total_successful'] / total * 100
        
        for period in period_summary:
            total = period_summary[period]['total_slots']
            if total > 0:
                period_summary[period]['participation_rate'] = period_summary[period]['total_successful'] / total * 100
        
        # Update metadata
        total_attestations = sum(stat['total_slots'] for stat in detailed_stats)
        total_successful_all = sum(stat['successful_attestations'] for stat in detailed_stats)
        total_missed_all = sum(stat['missed_attestations'] for stat in detailed_stats)
        overall_rate = (total_successful_all / total_attestations * 100) if total_attestations > 0 else 0
        
        # Save comprehensive data
        updated_data = {
            'metadata': {
                'last_updated': datetime.datetime.now().isoformat(),
                'total_periods_tracked': len(period_summary),
                'total_validators_in_committees': len(unique_validators),
                'total_attestations_tracked': total_attestations,
                'total_successful_attestations': total_successful_all,
                'total_missed_attestations': total_missed_all,
                'overall_participation_rate': round(overall_rate, 2)
            },
            'period_summary': dict(period_summary),
            'operator_summary': dict(operator_summary),
            'detailed_stats': detailed_stats
        }
        
        self._save_data(updated_data)
        
        print(f"\n=== SYNC COMMITTEE SCAN COMPLETE ===")
        print(f"  Periods processed: {len(periods_processed)}")
        print(f"  Validators in committees: {len(unique_validators)}")
        print(f"  Attestations checked: {total_attestations_checked:,}")
        print(f"  Successful: {total_successful:,}")
        print(f"  Missed: {total_missed:,}")
        if total_attestations_checked > 0:
            print(f"  Participation rate: {total_successful/total_attestations_checked*100:.2f}%")
        
        return len(periods_processed)

    def generate_report(self) -> None:
        """Generate comprehensive sync committee participation report."""
        if not os.path.exists(SYNC_COMMITTEE_DATA_FILE):
            print("No sync committee participation data available")
            return

        try:
            with open(SYNC_COMMITTEE_DATA_FILE, 'r') as f:
                data = json.load(f)

            metadata = data.get('metadata', {})
            period_summary = data.get('period_summary', {})
            operator_summary = data.get('operator_summary', {})
            detailed_stats = data.get('detailed_stats', [])

            print("\n" + "="*80)
            print("NODESET SYNC COMMITTEE PARTICIPATION REPORT")
            print("="*80)

            total_periods = metadata.get('total_periods_tracked', 0)
            total_validators = metadata.get('total_validators_in_committees', 0)
            total_attestations = metadata.get('total_attestations_tracked', 0)
            total_successful = metadata.get('total_successful_attestations', 0)
            total_missed = metadata.get('total_missed_attestations', 0)
            overall_rate = metadata.get('overall_participation_rate', 0)

            print(f"\n=== OVERALL STATISTICS ===")
            print(f"Sync committee periods tracked: {total_periods}")
            print(f"Validators in committees: {total_validators}")
            print(f"Total attestations tracked: {total_attestations:,}")
            print(f"Successful attestations: {total_successful:,}")
            print(f"Missed attestations: {total_missed:,}")
            print(f"Overall participation rate: {overall_rate:.2f}%")

            if operator_summary:
                print(f"\n=== OPERATOR PERFORMANCE ===")
                sorted_operators = sorted(
                    operator_summary.items(),
                    key=lambda x: x[1]['participation_rate'],
                    reverse=True
                )

                for operator, stats in sorted_operators:
                    ens_name = self.validator_data.get('ens_names', {}).get(operator)
                    display_name = f"{ens_name} ({operator[:8]}...)" if ens_name else f"{operator[:8]}..."
                    
                    periods = stats['total_periods']
                    slots = stats['total_slots']
                    successful = stats['total_successful']
                    missed = stats['total_missed']
                    rate = stats['participation_rate']

                    print(f"{display_name}")
                    print(f"  Periods: {periods}, Attestations: {successful:,}/{slots:,} ({rate:.2f}%)")

            if period_summary:
                print(f"\n=== PERIOD PERFORMANCE ===")
                sorted_periods = sorted(period_summary.items(), key=lambda x: int(x[0]))

                for period_str, stats in sorted_periods[-10:]:  # Show last 10 periods
                    period = int(period_str)
                    start_epoch = period * EPOCHS_PER_SYNC_COMMITTEE_PERIOD
                    end_epoch = start_epoch + EPOCHS_PER_SYNC_COMMITTEE_PERIOD - 1
                    
                    validators = stats['our_validators_count']
                    successful = stats['total_successful']
                    total = stats['total_slots']
                    rate = stats['participation_rate']

                    print(f"Period {period} (Epochs {start_epoch}-{end_epoch}): {validators} validators")
                    print(f"  Participation: {successful:,}/{total:,} ({rate:.2f}%)")

            # Show recent poor performance
            if detailed_stats:
                print(f"\n=== RECENT LOW PARTICIPATION (< 95%) ===")
                recent_poor = [
                    stat for stat in detailed_stats 
                    if stat['participation_rate'] < 95.0
                ]
                recent_poor.sort(key=lambda x: x['period'], reverse=True)

                for stat in recent_poor[:10]:
                    period = stat['period']
                    operator_name = stat['operator_name']
                    rate = stat['participation_rate']
                    successful = stat['successful_attestations']
                    total = stat['total_slots']
                    is_partial = stat['is_partial_period']

                    print(f"Period {period}: {operator_name}")
                    print(f"  {successful:,}/{total:,} ({rate:.2f}%) {'(partial)' if is_partial else ''}")

        except Exception as e:
            logging.error("Error generating sync committee report: %s", str(e))


def main():
    """Main execution function."""
    import sys
    
    beacon_api_url = os.getenv('BEACON_API_URL')

    if not beacon_api_url:
        raise ValueError("BEACON_API_URL environment variable is required")

    print("NodeSet Sync Committee Participation Tracker")
    print("Data source: Local Lighthouse beacon API")
    print("Tracks: Sync committee assignments and attestation performance")

    tracker = NodeSetSyncCommitteeTracker(beacon_api_url)
    
    # Check for debug flag
    debug_mode = '--debug' in sys.argv
    if debug_mode:
        print("\n=== DEBUG MODE ENABLED ===")
        tracker.debug_sync_committee_detection()
        return
    
    periods_found = tracker.scan_sync_committee_participation(debug_mode=False)
    tracker.generate_report()


if __name__ == "__main__":
    main()
