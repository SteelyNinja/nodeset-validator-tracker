#!/usr/bin/env python3
"""
NodeSet Validator Performance Tracker

Fetches performance data and activation information for all NodeSet validators 
using beaconcha.in API and stores it in a JSON cache file for analysis.
"""

import json
import requests
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
import os
import fcntl
import tempfile
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('validator_performance_tracker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ValidatorPerformanceTracker:
    def __init__(self, cache_file: str = "validator_performance_cache.json", batch_size: int = 100):
        self.cache_file = cache_file
        self.batch_size = batch_size
        self.base_url = "https://beaconcha.in/api/v1"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NodeSet-Validator-Performance-Tracker/1.0'
        })
        
        # Beacon chain constants for epoch to timestamp conversion
        self.GENESIS_TIMESTAMP = 1606824023  # Dec 1, 2020, 12:00:23 UTC
        self.SECONDS_PER_EPOCH = 32 * 12     # 384 seconds per epoch
        
        # Load existing cache
        self.cache = self.load_cache()
    
    def load_cache(self) -> Dict:
        """Load existing performance cache or create new structure"""
        if os.path.exists(self.cache_file):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with open(self.cache_file, 'r') as f:
                        fcntl.flock(f.fileno(), fcntl.LOCK_SH)  # Shared lock for reading
                        return json.load(f)
                except (json.JSONDecodeError, OSError) as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"JSON parse error on attempt {attempt + 1}: {e}. Retrying...")
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        logger.warning(f"Could not load cache file after {max_retries} attempts: {e}. Starting fresh.")
                except Exception as e:
                    logger.warning(f"Could not load cache file: {e}. Starting fresh.")
                    break
        
        return {
            "last_updated": None,
            "total_validators": 0,
            "validators": {}
        }
    
    def save_cache(self):
        """Save performance cache to file using atomic write"""
        self.cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        
        # Write to temporary file first (atomic write)
        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', dir=os.path.dirname(self.cache_file), 
                                           prefix=os.path.basename(self.cache_file) + '.tmp', 
                                           delete=False) as f:
                temp_file = f.name
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)  # Exclusive lock for writing
                json.dump(self.cache, f, indent=2)
            
            # Atomically replace the original file
            shutil.move(temp_file, self.cache_file)
            logger.info(f"Saved performance data to {self.cache_file}")
            
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
            if temp_file and os.path.exists(temp_file):
                os.unlink(temp_file)
            raise
    
    def load_nodeset_validators(self) -> Dict[str, List[str]]:
        """Load validator pubkeys from the main NodeSet tracker cache"""
        cache_file = "nodeset_validator_tracker_cache.json"
        
        if not os.path.exists(cache_file):
            raise FileNotFoundError(f"NodeSet validator cache not found: {cache_file}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with open(cache_file, 'r') as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)  # Shared lock for reading
                    data = json.load(f)
                break
            except (json.JSONDecodeError, OSError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"JSON parse error loading NodeSet cache on attempt {attempt + 1}: {e}. Retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
        
        if 'validator_pubkeys' not in data:
            raise ValueError("No validator_pubkeys found in NodeSet cache")
        
        return data['validator_pubkeys']
    
    def get_all_validator_data(self) -> Dict[int, Dict]:
        """Extract all validator indices and their metadata from NodeSet cache"""
        cache_file = "nodeset_validator_tracker_cache.json"
        
        if not os.path.exists(cache_file):
            raise FileNotFoundError(f"NodeSet validator cache not found: {cache_file}")
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with open(cache_file, 'r') as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH)  # Shared lock for reading
                    data = json.load(f)
                break
            except (json.JSONDecodeError, OSError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"JSON parse error loading validator data on attempt {attempt + 1}: {e}. Retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise
        
        if 'validator_indices' not in data:
            raise ValueError("No validator_indices found in NodeSet cache")
        
        validator_data = {}
        validator_indices = data['validator_indices']
        
        # Build mapping of validator_index -> {pubkey, operator}
        for pubkey, validator_index in validator_indices.items():
            operator = self.get_operator_for_pubkey(pubkey)
            validator_data[validator_index] = {
                'pubkey': pubkey,
                'operator': operator
            }
        
        logger.info(f"Found {len(validator_data)} validators across multiple operators")
        return validator_data
    
    def get_operator_for_pubkey(self, pubkey: str) -> str:
        """Get the operator address for a given validator pubkey"""
        validator_pubkeys = self.load_nodeset_validators()
        
        for operator, pubkeys in validator_pubkeys.items():
            if pubkey in pubkeys:
                return operator
        
        return "unknown"
    
    def epoch_to_timestamp(self, epoch: int) -> int:
        """Convert beacon chain epoch to Unix timestamp"""
        return self.GENESIS_TIMESTAMP + (epoch * self.SECONDS_PER_EPOCH)
    
    def epoch_to_datetime(self, epoch: int) -> datetime:
        """Convert beacon chain epoch to datetime object"""
        timestamp = self.epoch_to_timestamp(epoch)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    
    def fetch_validator_info_batch(self, validator_indices: List[int]) -> Optional[List[Dict]]:
        """Fetch basic validator info including activation data for a batch of validators"""
        if not validator_indices:
            return []
        
        # Join validator indices with commas for batch request
        indices_str = ','.join(map(str, validator_indices))
        url = f"{self.base_url}/validator/{indices_str}"
        
        try:
            logger.debug(f"Fetching validator info for {len(validator_indices)} validators")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK':
                result = data.get('data', [])
                # Debug logging to understand data format
                if result and not isinstance(result[0], dict):
                    logger.warning(f"Unexpected validator info data format. Expected dict, got {type(result[0])}: {result[0] if result else 'empty'}")
                return result
            else:
                logger.error(f"API error for validator info: {data}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching validator info batch: {e}")
            return None
    
    def fetch_performance_batch(self, validator_indices: List[int]) -> Optional[List[Dict]]:
        """Fetch performance data for a batch of validators using indices"""
        if not validator_indices:
            return []
        
        # Join validator indices with commas for batch request
        indices_str = ','.join(map(str, validator_indices))
        url = f"{self.base_url}/validator/{indices_str}/performance"
        
        try:
            logger.debug(f"Fetching performance for {len(validator_indices)} validators (indices)")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK':
                return data.get('data', [])
            else:
                logger.error(f"API error: {data}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching performance batch: {e}")
            return None
    
    def process_combined_data(self, performance_data: List[Dict], validator_info_data: List[Dict], validator_metadata: Dict[int, Dict]):
        """Process and store combined performance and validator info data"""
        if not performance_data and not validator_info_data:
            return
        
        # Create mappings by validator index
        perf_by_index = {p.get('validatorindex'): p for p in performance_data if isinstance(p, dict) and p.get('validatorindex')}
        info_by_index = {v.get('validatorindex'): v for v in validator_info_data if isinstance(v, dict) and v.get('validatorindex')}
        
        # Process all validators that have either performance or info data
        all_indices = set(perf_by_index.keys()) | set(info_by_index.keys())
        
        for validator_index in all_indices:
            # Get metadata for this validator
            metadata = validator_metadata.get(validator_index, {})
            pubkey = metadata.get('pubkey', 'unknown')
            operator = metadata.get('operator', 'unknown')
            
            # Get performance and info data
            perf = perf_by_index.get(validator_index, {})
            info = info_by_index.get(validator_index, {})
            
            # Process activation data
            activation_data = {}
            if info:
                activation_epoch = info.get('activationepoch')
                activation_eligibility_epoch = info.get('activationeligibilityepoch')
                exit_epoch = info.get('exitepoch')
                
                if activation_epoch and activation_epoch != 9223372036854775807:  # Max uint64 means not set
                    activation_data = {
                        "activation_epoch": activation_epoch,
                        "activation_timestamp": self.epoch_to_timestamp(activation_epoch),
                        "activation_date": self.epoch_to_datetime(activation_epoch).isoformat(),
                    }
                
                if activation_eligibility_epoch:
                    activation_data["activation_eligibility_epoch"] = activation_eligibility_epoch
                    activation_data["activation_eligibility_timestamp"] = self.epoch_to_timestamp(activation_eligibility_epoch)
                    activation_data["activation_eligibility_date"] = self.epoch_to_datetime(activation_eligibility_epoch).isoformat()
                
                if exit_epoch and exit_epoch != 9223372036854775807:  # Check if actually exited
                    activation_data["exit_epoch"] = exit_epoch
                    activation_data["exit_timestamp"] = self.epoch_to_timestamp(exit_epoch)
                    activation_data["exit_date"] = self.epoch_to_datetime(exit_epoch).isoformat()
                
                activation_data["status"] = info.get('status', 'unknown')
                activation_data["slashed"] = info.get('slashed', False)
            
            # Store combined data using pubkey as key
            validator_record = {
                "validator_index": validator_index,
                "operator": operator,
                "current_balance": perf.get('balance'),
                "performance_metrics": {
                    "performance_today": perf.get('performancetoday'),
                    "performance_1d": perf.get('performance1d'),
                    "performance_7d": perf.get('performance7d'),
                    "performance_31d": perf.get('performance31d'),
                    "performance_365d": perf.get('performance365d'),
                    "performance_total": perf.get('performancetotal'),
                    "rank_7d": perf.get('rank7d')
                },
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
            
            # Add activation data if available
            if activation_data:
                validator_record["activation_data"] = activation_data
            
            self.cache['validators'][pubkey] = validator_record
    
    def categorize_validators(self, validator_data: Dict[int, Dict]) -> tuple:
        """Categorize validators into new and existing for efficient processing"""
        new_validators = []
        existing_validators = []
        
        for validator_index, metadata in validator_data.items():
            pubkey = metadata.get('pubkey')
            
            # Check if we already have this validator with activation data
            if pubkey in self.cache['validators'] and 'activation_data' in self.cache['validators'][pubkey]:
                existing_validators.append(validator_index)
            else:
                new_validators.append(validator_index)
        
        return new_validators, existing_validators
    
    def process_existing_validators(self, performance_data: List[Dict], validator_metadata: Dict[int, Dict]):
        """Update performance data only for existing validators (skip activation data)"""
        if not performance_data:
            return
        
        for perf in performance_data:
            validator_index = perf.get('validatorindex')
            if not validator_index:
                continue
            
            # Get metadata for this validator
            metadata = validator_metadata.get(validator_index, {})
            pubkey = metadata.get('pubkey', 'unknown')
            
            # Update only performance metrics, preserve existing activation data
            if pubkey in self.cache['validators']:
                existing_record = self.cache['validators'][pubkey]
                existing_record['current_balance'] = perf.get('balance')
                existing_record['performance_metrics'] = {
                    "performance_today": perf.get('performancetoday'),
                    "performance_1d": perf.get('performance1d'),
                    "performance_7d": perf.get('performance7d'),
                    "performance_31d": perf.get('performance31d'),
                    "performance_365d": perf.get('performance365d'),
                    "performance_total": perf.get('performancetotal'),
                    "rank_7d": perf.get('rank7d')
                }
                existing_record['last_updated'] = datetime.now(timezone.utc).isoformat()
    
    def run_performance_collection(self):
        """Main function to collect performance data for all validators"""
        logger.info("Starting intelligent validator performance collection")
        
        # Get all validator data (indices and metadata)
        validator_data = self.get_all_validator_data()
        validator_indices = list(validator_data.keys())
        self.cache["total_validators"] = len(validator_indices)
        
        # Categorize validators for efficient processing
        new_validators, existing_validators = self.categorize_validators(validator_data)
        
        logger.info(f"Found {len(new_validators)} new validators requiring full data collection")
        logger.info(f"Found {len(existing_validators)} existing validators requiring only performance updates")
        
        total_processed = 0
        successful_batches = 0
        failed_batches = 0
        
        # Process new validators (need both performance and activation data)
        if new_validators:
            logger.info("Processing new validators with full data collection...")
            new_batches = (len(new_validators) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(new_validators), self.batch_size):
                batch_num = (i // self.batch_size) + 1
                batch_indices = new_validators[i:i + self.batch_size]
                batch_metadata = {idx: validator_data[idx] for idx in batch_indices}
                
                logger.info(f"Processing NEW validators batch {batch_num}/{new_batches} ({len(batch_indices)} validators)")
                
                # Fetch both performance and validator info data for new validators
                performance_data = self.fetch_performance_batch(batch_indices)
                validator_info_data = self.fetch_validator_info_batch(batch_indices)
                
                if performance_data is not None or validator_info_data is not None:
                    perf_count = len(performance_data) if performance_data else 0
                    info_count = len(validator_info_data) if validator_info_data else 0
                    
                    self.process_combined_data(performance_data or [], validator_info_data or [], batch_metadata)
                    successful_batches += 1
                    total_processed += len(batch_indices)
                    logger.info(f"Successfully processed NEW batch {batch_num} - got {perf_count} performance, {info_count} validator info results")
                else:
                    failed_batches += 1
                    logger.error(f"Failed to process NEW batch {batch_num}")
                
                # Save progress periodically
                if batch_num % 5 == 0:
                    self.save_cache()
                    logger.info(f"Saved progress after {batch_num} NEW validator batches")
                
                # Rate limiting for 2 API calls per batch
                time.sleep(1.5)
        
        # Process existing validators (performance data only, much faster)
        if existing_validators:
            logger.info("Processing existing validators with performance-only updates...")
            existing_batches = (len(existing_validators) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(existing_validators), self.batch_size):
                batch_num = (i // self.batch_size) + 1
                batch_indices = existing_validators[i:i + self.batch_size]
                batch_metadata = {idx: validator_data[idx] for idx in batch_indices}
                
                logger.info(f"Processing EXISTING validators batch {batch_num}/{existing_batches} ({len(batch_indices)} validators)")
                
                # Fetch only performance data for existing validators
                performance_data = self.fetch_performance_batch(batch_indices)
                
                if performance_data is not None:
                    self.process_existing_validators(performance_data, batch_metadata)
                    successful_batches += 1
                    total_processed += len(batch_indices)
                    logger.info(f"Successfully updated EXISTING batch {batch_num} - got {len(performance_data)} performance results")
                else:
                    failed_batches += 1
                    logger.error(f"Failed to process EXISTING batch {batch_num}")
                
                # Save progress periodically
                if batch_num % 10 == 0:
                    self.save_cache()
                    logger.info(f"Saved progress after {batch_num} EXISTING validator batches")
                
                # Rate limiting for 1 API call per batch (faster for existing)
                time.sleep(0.8)
        
        # Final save
        self.save_cache()
        
        logger.info(f"Intelligent performance collection completed")
        logger.info(f"New validators processed: {len(new_validators)}")
        logger.info(f"Existing validators updated: {len(existing_validators)}")
        logger.info(f"Total validators processed: {total_processed}")
        logger.info(f"Successful batches: {successful_batches}")
        logger.info(f"Failed batches: {failed_batches}")
        logger.info(f"API calls saved by skipping activation data: {len(existing_validators) // self.batch_size}")
        
        # Calculate time savings
        if existing_validators:
            estimated_time_saved = (len(existing_validators) // self.batch_size) * 0.7  # 0.7 seconds saved per batch
            logger.info(f"Estimated time saved: {estimated_time_saved:.1f} seconds")
    
    def generate_summary_stats(self):
        """Generate summary statistics from performance data"""
        if not self.cache['validators']:
            logger.warning("No validator data available for summary")
            return
        
        validators = self.cache['validators']
        
        # Calculate aggregated metrics
        total_performance_1d = sum(v['performance_metrics']['performance_1d'] or 0 for v in validators.values())
        total_performance_7d = sum(v['performance_metrics']['performance_7d'] or 0 for v in validators.values())
        total_performance_31d = sum(v['performance_metrics']['performance_31d'] or 0 for v in validators.values())
        total_performance_total = sum(v['performance_metrics']['performance_total'] or 0 for v in validators.values())
        
        avg_performance_1d = total_performance_1d / len(validators) if validators else 0
        avg_performance_7d = total_performance_7d / len(validators) if validators else 0
        avg_performance_31d = total_performance_31d / len(validators) if validators else 0
        
        summary = {
            "total_validators": len(validators),
            "aggregate_performance": {
                "total_1d": total_performance_1d,
                "total_7d": total_performance_7d,
                "total_31d": total_performance_31d,
                "total_lifetime": total_performance_total
            },
            "average_performance": {
                "avg_1d": avg_performance_1d,
                "avg_7d": avg_performance_7d,
                "avg_31d": avg_performance_31d
            },
            "performance_in_eth": {
                "total_1d_eth": total_performance_1d / 1e18,
                "total_7d_eth": total_performance_7d / 1e18,
                "total_31d_eth": total_performance_31d / 1e18,
                "total_lifetime_eth": total_performance_total / 1e18,
                "avg_daily_eth": avg_performance_1d / 1e18,
                "avg_validator_lifetime_eth": (total_performance_total / len(validators)) / 1e18 if validators else 0
            }
        }
        
        logger.info("Performance Summary:")
        logger.info(f"  Total validators: {summary['total_validators']}")
        logger.info(f"  Total 1d rewards: {summary['performance_in_eth']['total_1d_eth']:.6f} ETH")
        logger.info(f"  Total 7d rewards: {summary['performance_in_eth']['total_7d_eth']:.6f} ETH")
        logger.info(f"  Total 31d rewards: {summary['performance_in_eth']['total_31d_eth']:.6f} ETH")
        logger.info(f"  Total lifetime rewards: {summary['performance_in_eth']['total_lifetime_eth']:.6f} ETH")
        logger.info(f"  Average daily per validator: {summary['performance_in_eth']['avg_daily_eth']:.6f} ETH")
        
        return summary

def main():
    """Main execution function"""
    try:
        tracker = ValidatorPerformanceTracker(batch_size=100)
        tracker.run_performance_collection()
        tracker.generate_summary_stats()
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
