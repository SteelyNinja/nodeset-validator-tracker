#!/usr/bin/env python3
"""
Operator Daily Performance Tracker

Generates daily attestation performance data for NodeSet operators over the last 365 days.
Only processes complete UTC days and stores data in a single JSON file for dashboard consumption.

Features:
- UTC day-based data collection (complete days only)
- Incremental updates (only processes new complete days)
- Attestation-only performance metrics (no proposals/sync committee)
- Head/Target/Source accuracy tracking
- Average inclusion delay calculation
- Performance vs theoretical maximum calculation
"""

import json
import requests
import logging
import os
import fcntl
import tempfile
import shutil
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from urllib.parse import quote

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # dotenv not available, rely on system environment
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('operator_daily_performance_tracker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ClickHouseHTTPClient:
    """Simple HTTP client for ClickHouse queries"""
    
    def __init__(self, host: str = "localhost", port: int = 8123, timeout: int = 30):
        self.base_url = f"http://{host}:{port}"
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NodeSet-Daily-Performance-Tracker/1.0'
        })
        
        # Beacon chain constants
        self.GENESIS_TIMESTAMP = 1606824023  # Dec 1, 2020, 12:00:23 UTC
        self.SECONDS_PER_EPOCH = 32 * 12     # 384 seconds per epoch
        self.EPOCHS_PER_DAY = 225            # ~225 epochs per day
    
    def execute_query(self, query: str) -> List[List[str]]:
        """Execute ClickHouse query via HTTP interface"""
        try:
            logger.debug(f"Executing query: {query[:100]}...")
            
            response = self.session.get(
                f"{self.base_url}/",
                params={'query': query},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            # Parse TSV response
            return self._parse_tsv_response(response.text)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"ClickHouse query failed: {e}")
            raise
    
    def _parse_tsv_response(self, tsv_data: str) -> List[List[str]]:
        """Parse TSV response into list of lists"""
        lines = tsv_data.strip().split('\n')
        if not lines or lines == ['']:
            return []
            
        data = []
        for line in lines:
            if line.strip():
                values = line.split('\t')
                data.append(values)
        
        return data
    
    def epoch_to_timestamp(self, epoch: int) -> int:
        """Convert beacon chain epoch to Unix timestamp"""
        return self.GENESIS_TIMESTAMP + (epoch * self.SECONDS_PER_EPOCH)
    
    def epoch_to_datetime(self, epoch: int) -> datetime:
        """Convert beacon chain epoch to datetime object"""
        timestamp = self.epoch_to_timestamp(epoch)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    
    def timestamp_to_epoch(self, timestamp: int) -> int:
        """Convert Unix timestamp to beacon chain epoch"""
        return (timestamp - self.GENESIS_TIMESTAMP) // self.SECONDS_PER_EPOCH
    
    def get_utc_day_epoch_range(self, date: datetime) -> tuple:
        """Get the epoch range for a complete UTC day"""
        # Start of day (00:00:00 UTC)
        day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        # End of day (23:59:59 UTC)
        day_end = day_start + timedelta(days=1) - timedelta(seconds=1)
        
        start_epoch = self.timestamp_to_epoch(int(day_start.timestamp()))
        end_epoch = self.timestamp_to_epoch(int(day_end.timestamp()))
        
        return start_epoch, end_epoch
    
    def is_available(self) -> bool:
        """Check if ClickHouse is available"""
        try:
            response = self.session.get(
                f"{self.base_url}/",
                params={'query': 'SELECT 1'},
                timeout=5
            )
            return response.status_code == 200
        except Exception:
            return False

class OperatorDailyPerformanceTracker:
    """Tracks daily attestation performance for NodeSet operators"""
    
    def __init__(self, 
                 cache_file: str = "operator_daily_performance_cache.json",
                 clickhouse_host: str = "localhost",
                 clickhouse_port: int = 8123,
                 max_days: int = 365):
        self.cache_file = cache_file
        self.max_days = max_days
        self.clickhouse = ClickHouseHTTPClient(clickhouse_host, clickhouse_port)
        
        # Load existing cache
        self.cache = self.load_cache()
    
    def load_cache(self) -> Dict:
        """Load existing performance cache or create new structure"""
        if os.path.exists(self.cache_file):
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with open(self.cache_file, 'r') as f:
                        fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                        return json.load(f)
                except (json.JSONDecodeError, OSError) as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"JSON parse error on attempt {attempt + 1}: {e}. Retrying...")
                    else:
                        logger.warning(f"Could not load cache file after {max_retries} attempts: {e}. Starting fresh.")
                except Exception as e:
                    logger.warning(f"Could not load cache file: {e}. Starting fresh.")
                    break
        
        return {
            "last_updated": None,
            "data_period_days": self.max_days,
            "operators": {}
        }
    
    def save_cache(self):
        """Save performance cache to file using atomic write"""
        self.cache["last_updated"] = datetime.now(timezone.utc).isoformat()
        
        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', dir=os.path.dirname(self.cache_file), 
                                           prefix=os.path.basename(self.cache_file) + '.tmp', 
                                           delete=False) as f:
                temp_file = f.name
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(self.cache, f, indent=2)
            
            shutil.move(temp_file, self.cache_file)
            logger.info(f"Saved daily performance data to {self.cache_file}")
            
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
            if temp_file and os.path.exists(temp_file):
                os.unlink(temp_file)
            raise
    
    def get_complete_utc_days_available(self) -> List[datetime]:
        """Get list of complete UTC days that have data in the database"""
        try:
            # Get the epoch range available in database
            query = "SELECT MIN(epoch), MAX(epoch) FROM validators_summary WHERE val_nos_name IS NOT NULL"
            raw_data = self.clickhouse.execute_query(query)
            
            if not raw_data or len(raw_data[0]) < 2:
                logger.error("Could not get epoch range from database")
                return []
            
            min_epoch = int(raw_data[0][0])
            max_epoch = int(raw_data[0][1])
            
            # Convert to datetime
            min_date = self.clickhouse.epoch_to_datetime(min_epoch).replace(hour=0, minute=0, second=0, microsecond=0)
            max_date = self.clickhouse.epoch_to_datetime(max_epoch).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Only include complete days (not today if it's not complete)
            now_utc = datetime.now(timezone.utc)
            today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # If we're still in today, exclude today from complete days
            if max_date >= today_start:
                max_date = today_start - timedelta(days=1)
            
            # Generate list of complete days
            complete_days = []
            current_date = max_date
            
            while current_date >= min_date and len(complete_days) < self.max_days:
                complete_days.append(current_date)
                current_date -= timedelta(days=1)
            
            logger.info(f"Found {len(complete_days)} complete UTC days available in database")
            logger.info(f"Date range: {complete_days[-1].date()} to {complete_days[0].date()}")
            
            return complete_days
            
        except Exception as e:
            logger.error(f"Failed to get complete UTC days: {e}")
            return []
    
    def get_days_needing_update(self, available_days: List[datetime]) -> List[datetime]:
        """Determine which days need to be processed or updated"""
        if not available_days:
            return []
        
        # Return all available days - let update_operator_data handle per-operator deduplication
        logger.info(f"Found {len(available_days)} days available for processing")
        return available_days
    
    def get_operators_list(self) -> List[str]:
        """Get list of NodeSet operators from database"""
        try:
            query = "SELECT DISTINCT val_nos_name FROM validators_summary WHERE val_nos_name IS NOT NULL ORDER BY val_nos_name"
            raw_data = self.clickhouse.execute_query(query)
            
            operators = [row[0] for row in raw_data if row[0]]
            logger.info(f"Found {len(operators)} NodeSet operators")
            return operators
            
        except Exception as e:
            logger.error(f"Failed to get operators list: {e}")
            return []
    
    def calculate_daily_performance(self, operator: str, date: datetime) -> Optional[Dict[str, Any]]:
        """Calculate daily attestation performance for a specific operator and date"""
        try:
            # Get epoch range for this UTC day
            start_epoch, end_epoch = self.clickhouse.get_utc_day_epoch_range(date)
            
            query = f"""
            WITH daily_data AS (
                SELECT 
                    val_id,
                    val_nos_name,
                    epoch,
                    val_status,
                    att_happened,
                    att_valid_head,
                    att_valid_target,
                    att_valid_source,
                    att_inc_delay,
                    att_earned_reward,
                    att_missed_reward,
                    att_penalty,
                    -- Flag active duty periods
                    CASE WHEN val_status = 'active_ongoing' THEN 1 ELSE 0 END as is_active_duty,
                    -- Flag successful attestations
                    CASE WHEN val_status = 'active_ongoing' AND att_happened = 1 THEN 1 ELSE 0 END as successful_attestation,
                    -- Flag missed attestations (active but no attestation)
                    CASE WHEN val_status = 'active_ongoing' AND (att_happened = 0 OR att_happened IS NULL) THEN 1 ELSE 0 END as missed_attestation
                FROM validators_summary
                WHERE epoch >= {start_epoch}
                AND epoch <= {end_epoch}
                AND val_nos_name = '{operator}'
                AND val_status NOT IN ('exited', 'withdrawal_possible', 'withdrawal_done')
            )
            SELECT 
                val_nos_name,
                COUNT(DISTINCT val_id) as validator_count,
                
                -- Duty and attestation counts
                SUM(is_active_duty) as active_duty_periods,
                SUM(successful_attestation) as successful_attestations,
                SUM(missed_attestation) as missed_attestations,
                
                -- Head/Target/Source accuracy (only for active duties with attestations)
                SUM(CASE WHEN is_active_duty = 1 AND att_happened = 1 AND att_valid_head = 1 THEN 1 ELSE 0 END) as head_correct,
                SUM(CASE WHEN is_active_duty = 1 AND att_happened = 1 AND att_valid_target = 1 THEN 1 ELSE 0 END) as target_correct,
                SUM(CASE WHEN is_active_duty = 1 AND att_happened = 1 AND att_valid_source = 1 THEN 1 ELSE 0 END) as source_correct,
                SUM(CASE WHEN is_active_duty = 1 AND att_happened = 1 THEN 1 ELSE 0 END) as total_attestations_made,
                
                -- Inclusion delay (only for successful attestations)
                AVG(CASE WHEN is_active_duty = 1 AND att_happened = 1 AND att_inc_delay IS NOT NULL THEN att_inc_delay ELSE NULL END) as avg_inclusion_delay,
                
                -- Rewards and penalties (only for active duties)
                SUM(CASE WHEN is_active_duty = 1 THEN COALESCE(att_earned_reward, 0) ELSE 0 END) as total_earned_rewards,
                SUM(CASE WHEN is_active_duty = 1 THEN COALESCE(att_penalty, 0) ELSE 0 END) as total_penalties,
                
                -- Calculate average reward per successful attestation for theoretical calculation
                AVG(CASE WHEN successful_attestation = 1 AND att_earned_reward IS NOT NULL THEN att_earned_reward END) as avg_reward_per_attestation
                
            FROM daily_data
            GROUP BY val_nos_name
            """
            
            raw_data = self.clickhouse.execute_query(query)
            
            if not raw_data or len(raw_data) == 0:
                # No data for this operator on this day
                return None
            
            row = raw_data[0]
            
            # Extract data with safe conversions
            def safe_int(value):
                return int(value) if value not in ['\\N', None, ''] else 0
            
            def safe_float(value):
                return float(value) if value not in ['\\N', None, ''] else 0.0
            
            validator_count = safe_int(row[1])
            active_duty_periods = safe_int(row[2])
            successful_attestations = safe_int(row[3])
            missed_attestations = safe_int(row[4])
            head_correct = safe_int(row[5])
            target_correct = safe_int(row[6])
            source_correct = safe_int(row[7])
            total_attestations_made = safe_int(row[8])
            avg_inclusion_delay = safe_float(row[9])
            total_earned_rewards = safe_int(row[10])
            total_penalties = safe_int(row[11])
            avg_reward_per_attestation = safe_float(row[12])
            
            # Calculate performance metrics
            participation_rate = (successful_attestations / active_duty_periods * 100) if active_duty_periods > 0 else 0.0
            
            # Accuracy percentages (only for attestations that were made)
            head_accuracy = (head_correct / total_attestations_made * 100) if total_attestations_made > 0 else 0.0
            target_accuracy = (target_correct / total_attestations_made * 100) if total_attestations_made > 0 else 0.0
            source_accuracy = (source_correct / total_attestations_made * 100) if total_attestations_made > 0 else 0.0
            
            # Performance vs theoretical (net rewards vs max possible)
            net_rewards = total_earned_rewards - total_penalties
            max_possible_rewards = active_duty_periods * avg_reward_per_attestation
            attestation_performance = (net_rewards / max_possible_rewards * 100) if max_possible_rewards > 0 else 0.0
            
            return {
                "date": date.strftime("%Y-%m-%d"),
                "epoch_range": [start_epoch, end_epoch],
                "validator_count": validator_count,
                "active_duty_periods": active_duty_periods,
                "successful_attestations": successful_attestations,
                "missed_attestations": missed_attestations,
                "participation_rate": round(participation_rate, 2),
                "head_accuracy": round(head_accuracy, 2),
                "target_accuracy": round(target_accuracy, 2),
                "source_accuracy": round(source_accuracy, 2),
                "avg_inclusion_delay": round(avg_inclusion_delay, 3),
                "attestation_performance": round(attestation_performance, 8),
                "total_earned_rewards": total_earned_rewards,
                "total_penalties": total_penalties,
                "net_rewards": net_rewards,
                "max_possible_rewards": int(max_possible_rewards)
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate daily performance for {operator} on {date.date()}: {e}")
            return None
    
    def update_operator_data(self, operator: str, days_to_process: List[datetime]):
        """Update daily performance data for a specific operator"""
        if operator not in self.cache["operators"]:
            self.cache["operators"][operator] = {
                "daily_performance": []
            }
        
        operator_data = self.cache["operators"][operator]
        existing_dates = {day["date"] for day in operator_data["daily_performance"]}
        
        new_days_added = 0
        for date in days_to_process:
            date_str = date.strftime("%Y-%m-%d")
            
            if date_str not in existing_dates:
                daily_perf = self.calculate_daily_performance(operator, date)
                if daily_perf:
                    operator_data["daily_performance"].append(daily_perf)
                    new_days_added += 1
                else:
                    # Cache empty result for operators with no active validators on this date
                    # This prevents repeated expensive queries for exited operators
                    start_epoch, end_epoch = self.clickhouse.get_utc_day_epoch_range(date)
                    empty_entry = {
                        "date": date_str,
                        "epoch_range": [start_epoch, end_epoch],
                        "validator_count": 0,
                        "active_duty_periods": 0,
                        "successful_attestations": 0,
                        "missed_attestations": 0,
                        "participation_rate": 0.0,
                        "head_accuracy": 0.0,
                        "target_accuracy": 0.0,
                        "source_accuracy": 0.0,
                        "avg_inclusion_delay": 0.0,
                        "attestation_performance": 0.0,
                        "total_earned_rewards": 0,
                        "total_penalties": 0,
                        "net_rewards": 0,
                        "max_possible_rewards": 0,
                        "no_active_validators": True  # Flag to indicate this is an empty result
                    }
                    operator_data["daily_performance"].append(empty_entry)
                    new_days_added += 1
        
        # Sort by date (newest first) and limit to max_days
        operator_data["daily_performance"].sort(key=lambda x: x["date"], reverse=True)
        operator_data["daily_performance"] = operator_data["daily_performance"][:self.max_days]
        
        if new_days_added > 0:
            logger.info(f"Added {new_days_added} new days for operator {operator}")
        
        return new_days_added
    
    def run_daily_update(self, test_operators: int = None):
        """Main function to update daily performance data"""
        logger.info("Starting operator daily performance update")
        
        if not self.clickhouse.is_available():
            logger.error("ClickHouse is not available")
            return
        
        # Get available complete days
        available_days = self.get_complete_utc_days_available()
        if not available_days:
            logger.error("No complete days available in database")
            return
        
        # Get all available days - each operator will check individually for missing days
        days_to_process = self.get_days_needing_update(available_days)
        
        # Get list of operators
        operators = self.get_operators_list()
        if not operators:
            logger.error("No operators found in database")
            return
        
        # Limit operators for testing
        if test_operators:
            operators = operators[:test_operators]
            logger.info(f"TESTING MODE: Processing only first {test_operators} operators")
        
        logger.info(f"Processing {len(days_to_process)} days for {len(operators)} operators")
        
        total_updates = 0
        start_time = datetime.now(timezone.utc)
        
        for i, operator in enumerate(operators, 1):
            op_start = datetime.now(timezone.utc)
            logger.info(f"Processing operator {i}/{len(operators)}: {operator}")
            
            updates = self.update_operator_data(operator, days_to_process)
            total_updates += updates
            
            op_duration = (datetime.now(timezone.utc) - op_start).total_seconds()
            
            # Calculate ETA
            avg_time_per_op = (datetime.now(timezone.utc) - start_time).total_seconds() / i
            remaining_ops = len(operators) - i
            eta_seconds = remaining_ops * avg_time_per_op
            eta_minutes = eta_seconds / 60
            
            logger.info(f"Operator {i} completed in {op_duration:.1f}s. ETA: {eta_minutes:.1f} minutes ({remaining_ops} operators remaining)")
            
            # Save progress periodically
            if i % 10 == 0:
                self.save_cache()
                elapsed_minutes = (datetime.now(timezone.utc) - start_time).total_seconds() / 60
                logger.info(f"=== CHECKPOINT {i}/{len(operators)} === Elapsed: {elapsed_minutes:.1f}min, ETA: {eta_minutes:.1f}min ===")
            
            # Progress milestones
            if i in [25, 50, 75, 100, 125, 150]:
                progress_pct = (i / len(operators)) * 100
                elapsed_minutes = (datetime.now(timezone.utc) - start_time).total_seconds() / 60
                logger.info(f"🚀 MILESTONE: {progress_pct:.1f}% complete ({i}/{len(operators)}) - {elapsed_minutes:.1f}min elapsed")
        
        # Final save
        self.save_cache()
        
        logger.info(f"Daily performance update completed")
        logger.info(f"Total operators processed: {len(operators)}")
        logger.info(f"Total daily updates added: {total_updates}")

def main():
    """Main execution function"""
    try:
        # Configuration - adjust these as needed
        clickhouse_host = os.getenv("CLICKHOUSE_HOST", "192.168.202.250")
        clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        test_mode = os.getenv("TEST_OPERATORS")
        
        tracker = OperatorDailyPerformanceTracker(
            clickhouse_host=clickhouse_host,
            clickhouse_port=clickhouse_port,
            max_days=365
        )
        
        if test_mode:
            tracker.run_daily_update(test_operators=int(test_mode))
        else:
            tracker.run_daily_update()
        
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        raise

if __name__ == "__main__":
    main()