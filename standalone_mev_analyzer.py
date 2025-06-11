#!/usr/bin/env python3
"""
Standalone MEV Relay Analyzer
Analyzes NodeSet validators across all major MEV relays and outputs comprehensive results.

Usage: python standalone_mev_analyzer.py
"""

import asyncio
import aiohttp
import requests
import time
import json
import logging
import os
from typing import Dict, List, Optional, Set
from collections import defaultdict
from datetime import datetime

# Configuration
CACHE_FILE = "nodeset_validator_tracker_cache.json"
OUTPUT_FILE = "mev_analysis_results.json"
LOG_FILE = "mev_analyzer.log"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Working MEV Relays with corrected endpoints
WORKING_MEV_RELAYS = {
    'flashbots': {
        'name': 'Flashbots',
        'endpoint': 'https://boost-relay.flashbots.net/relay/v1/data/validator_registration',
        'headers': {
            'Accept': 'application/json',
            'User-Agent': 'MEV-Analyzer/1.0'
        },
        'rate_limit': 0.1,
        'active': True
    },
    'bloxroute_max_profit': {
        'name': 'bloXroute Max Profit',
        'endpoint': 'https://bloxroute.max-profit.blxrbdn.com/relay/v1/data/validator_registration',
        'headers': {
            'Accept': 'application/json',
            'User-Agent': 'MEV-Analyzer/1.0'
        },
        'rate_limit': 0.05,
        'active': True
    },
    'bloxroute_regulated': {
        'name': 'bloXroute Regulated',
        'endpoint': 'https://bloxroute.regulated.blxrbdn.com/relay/v1/data/validator_registration',
        'headers': {
            'Accept': 'application/json',
            'User-Agent': 'MEV-Analyzer/1.0'
        },
        'rate_limit': 0.05,
        'active': True
    },
    'bloxroute_ethical': {
        'name': 'bloXroute Ethical',
        'endpoint': 'https://bloxroute.ethical.blxrbdn.com/relay/v1/data/validator_registration',
        'headers': {
            'Accept': 'application/json',
            'User-Agent': 'MEV-Analyzer/1.0'
        },
        'rate_limit': 0.05,
        'active': True
    },
    'ultrasound': {
        'name': 'Ultra Sound',
        'endpoint': 'https://relay.ultrasound.money/relay/v1/data/validator_registration',
        'headers': {
            'Accept': 'application/json',
            'User-Agent': 'MEV-Analyzer/1.0'
        },
        'rate_limit': 0.1,
        'active': True
    },
    'aestus': {
        'name': 'Aestus',
        'endpoint': 'https://aestus.live/relay/v1/data/validator_registration',
        'headers': {
            'Accept': 'application/json',
            'User-Agent': 'MEV-Analyzer/1.0'
        },
        'rate_limit': 0.1,
        'active': True
    },
    'securerpc': {
        'name': 'SecureRPC',
        'endpoint': 'https://mainnet-relay.securerpc.com/relay/v1/data/validator_registration',
        'headers': {
            'Accept': 'application/json',
            'User-Agent': 'MEV-Analyzer/1.0'
        },
        'rate_limit': 0.1,
        'active': True
    },
    'agnostic': {
        'name': 'Agnostic Relay',
        'endpoint': 'https://agnostic-relay.net/relay/v1/data/validator_registration',
        'headers': {
            'Accept': 'application/json',
            'User-Agent': 'MEV-Analyzer/1.0'
        },
        'rate_limit': 0.1,
        'active': True
    }
}

class StandaloneMEVAnalyzer:
    """Standalone MEV analyzer that reads from cache and outputs to JSON."""
    
    def __init__(self):
        self.cache_data = self._load_cache()
        self.working_relays = {}
        self.failed_relays = {}
        self.all_found_validators = set()
        self.analysis_start_time = datetime.now()
        
    def _load_cache(self) -> dict:
        """Load cached validator data from the main tracker."""
        if not os.path.exists(CACHE_FILE):
            raise FileNotFoundError(f"Cache file {CACHE_FILE} not found. Run the main tracker first.")
        
        try:
            with open(CACHE_FILE, 'r') as f:
                cache = json.load(f)
                logging.info(f"Loaded cache with {cache.get('total_validators', 0)} validators")
                return cache
        except Exception as e:
            raise Exception(f"Error loading cache: {str(e)}")
    
    def _get_validator_pubkeys(self) -> List[str]:
        """Get active validator public keys from cache."""
        validator_indices = self.cache_data.get('validator_indices', {})
        exited_pubkeys = set(self.cache_data.get('exited_pubkeys', []))
        
        active_pubkeys = [pk for pk in validator_indices.keys() if pk not in exited_pubkeys]
        logging.info(f"Found {len(active_pubkeys)} active validator pubkeys")
        return active_pubkeys
    
    def _get_operator_info(self) -> Dict[str, any]:
        """Get operator information from cache."""
        return {
            'operator_validators': self.cache_data.get('operator_validators', {}),
            'validator_pubkeys': self.cache_data.get('validator_pubkeys', {}),
            'ens_names': self.cache_data.get('ens_names', {}),
            'total_validators': self.cache_data.get('total_validators', 0)
        }
    
    def format_operator_display(self, address: str) -> str:
        """Format operator address with ENS name if available."""
        ens_names = self.cache_data.get('ens_names', {})
        ens_name = ens_names.get(address)
        if ens_name:
            return f"{ens_name} ({address[:8]}...{address[-6:]})"
        else:
            return f"{address[:8]}...{address[-6:]}"
    
    def test_relay_health(self) -> Dict[str, dict]:
        """Test all relays to find working ones."""
        print("🩺 Testing relay connectivity...")
        
        # Get a real validator pubkey for testing
        validator_pubkeys = self._get_validator_pubkeys()
        test_pubkey = validator_pubkeys[0] if validator_pubkeys else "0xb606e206c2bf3b78f53ebff8be8e8d4af2f0da68646b5642c4d511b15ab5ddb122ae57b48eab614f8ca5bafbe75a5999"
        
        working_relays = {}
        
        for relay_name, relay_config in WORKING_MEV_RELAYS.items():
            try:
                url = f"{relay_config['endpoint']}?pubkey={test_pubkey}"
                
                response = requests.get(
                    url,
                    headers=relay_config['headers'],
                    timeout=15
                )
                
                # 200 = found, 404 = not found (both mean API is working)
                is_working = response.status_code in [200, 404]
                
                if is_working:
                    working_relays[relay_name] = relay_config
                    self.working_relays[relay_name] = relay_config
                    
                    if response.status_code == 200:
                        print(f"   📝 {relay_config['name']}: HTTP 200 (found test validator)")
                    else:
                        print(f"   ✅ {relay_config['name']}: HTTP 404 (API working)")
                else:
                    self.failed_relays[relay_name] = f"HTTP {response.status_code}"
                    print(f"   ❌ {relay_config['name']}: HTTP {response.status_code}")
                    
            except Exception as e:
                self.failed_relays[relay_name] = str(e)
                print(f"   ❌ {relay_config['name']}: {str(e)}")
                
            time.sleep(0.2)  # Brief pause between tests
        
        print(f"\n📊 Working relays: {len(working_relays)}/{len(WORKING_MEV_RELAYS)}")
        return working_relays
    
    async def query_all_relays(self, validator_pubkeys: List[str]) -> Dict[str, Dict[str, dict]]:
        """Query all working relays for validator registrations."""
        
        working_relays = self.test_relay_health()
        
        if not working_relays:
            print("❌ No working relays found!")
            return {}
        
        print(f"\n🎯 Querying {len(working_relays)} relays for {len(validator_pubkeys):,} validators")
        
        all_results = {}
        
        for relay_name, relay_config in working_relays.items():
            print(f"\n🔍 Querying {relay_config['name']}...")
            
            try:
                registrations = await self._query_single_relay(
                    relay_name, relay_config, validator_pubkeys
                )
                
                if registrations:
                    all_results[relay_name] = registrations
                    self.all_found_validators.update(registrations.keys())
                    print(f"   ✅ Found {len(registrations):,} registrations")
                else:
                    print(f"   ⚠️  No registrations found")
                    
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
                logging.error(f"Error querying {relay_name}: {str(e)}")
                continue
        
        return all_results
    
    async def _query_single_relay(self, relay_name: str, relay_config: dict, 
                                 validator_pubkeys: List[str]) -> Dict[str, dict]:
        """Query a single relay with proper rate limiting."""
        
        registrations = {}
        rate_limit = relay_config.get('rate_limit', 0.1)
        
        # Controlled concurrency
        semaphore = asyncio.Semaphore(5)
        
        async def query_validator(session: aiohttp.ClientSession, pubkey: str):
            async with semaphore:
                try:
                    url = f"{relay_config['endpoint']}?pubkey={pubkey}"
                    
                    async with session.get(url, headers=relay_config['headers']) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data:
                                return pubkey, data
                        elif response.status == 429:
                            # Rate limited - wait longer
                            await asyncio.sleep(2.0)
                            
                except Exception as e:
                    logging.debug(f"Error querying {pubkey} on {relay_name}: {str(e)}")
                
                await asyncio.sleep(rate_limit)
                return pubkey, None
        
        # Execute with progress tracking
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [query_validator(session, pubkey) for pubkey in validator_pubkeys]
            
            # Process in smaller chunks for better progress tracking
            chunk_size = 50
            for i in range(0, len(tasks), chunk_size):
                chunk = tasks[i:i + chunk_size]
                results = await asyncio.gather(*chunk, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, tuple) and result[1] is not None:
                        pubkey, data = result
                        registrations[pubkey] = data
                
                # Progress update
                processed = min(i + chunk_size, len(tasks))
                if processed % 200 == 0 or processed == len(tasks):
                    print(f"      Progress: {processed:,}/{len(tasks):,} ({len(registrations):,} found)")
                
                # Brief pause between chunks
                if i + chunk_size < len(tasks):
                    await asyncio.sleep(0.5)
        
        return registrations
    
    def _extract_gas_limit(self, registration_data: dict) -> Optional[int]:
        """Extract gas limit from registration data."""
        try:
            message = registration_data.get('message', {})
            gas_limit_fields = ['gas_limit', 'gasLimit', 'max_gas_limit', 'preferred_gas_limit']
            
            for field in gas_limit_fields:
                if field in message:
                    gas_limit = message[field]
                    if isinstance(gas_limit, str):
                        return int(gas_limit)
                    elif isinstance(gas_limit, int):
                        return gas_limit
        except Exception as e:
            logging.debug(f"Error extracting gas limit: {str(e)}")
        return None
    
    def analyze_results(self, all_relay_data: Dict[str, Dict[str, dict]]) -> Dict[str, any]:
        """Comprehensive analysis of multi-relay results."""
        
        if not all_relay_data:
            return {}
        
        # Track coverage and patterns
        validator_coverage = defaultdict(set)
        gas_limit_data = defaultdict(dict)
        total_unique = set()
        
        for relay_name, registrations in all_relay_data.items():
            for pubkey, registration in registrations.items():
                validator_coverage[pubkey].add(relay_name)
                total_unique.add(pubkey)
                
                # Extract gas limit
                gas_limit = self._extract_gas_limit(registration)
                if gas_limit:
                    gas_limit_data[pubkey][relay_name] = gas_limit
        
        # Analysis results
        analysis = {
            'total_unique_validators': len(total_unique),
            'relay_coverage': {},
            'gas_limit_consistency': {},
            'gas_limit_distribution': defaultdict(int),
            'operator_analysis': self._analyze_operators(all_relay_data, gas_limit_data)
        }
        
        # Per-relay coverage stats
        for relay_name in all_relay_data.keys():
            found_count = len(all_relay_data[relay_name])
            coverage_pct = (found_count / len(total_unique) * 100) if total_unique else 0
            
            analysis['relay_coverage'][relay_name] = {
                'validators_found': found_count,
                'coverage_percentage': coverage_pct,
                'relay_display_name': WORKING_MEV_RELAYS[relay_name]['name']
            }
        
        # Gas limit consistency and distribution
        consistent_count = 0
        inconsistent_count = 0
        
        for pubkey, relay_limits in gas_limit_data.items():
            if len(set(relay_limits.values())) <= 1:
                consistent_count += 1
                # Add to distribution
                if relay_limits:
                    gas_limit = list(relay_limits.values())[0]
                    analysis['gas_limit_distribution'][gas_limit] += 1
            else:
                inconsistent_count += 1
                analysis['gas_limit_consistency'][pubkey] = relay_limits
        
        analysis['consistency_stats'] = {
            'consistent': consistent_count,
            'inconsistent': inconsistent_count,
            'consistency_rate': (consistent_count / (consistent_count + inconsistent_count) * 100) 
                              if (consistent_count + inconsistent_count) > 0 else 0
        }
        
        return analysis
    
    def _analyze_operators(self, all_relay_data: Dict[str, Dict[str, dict]], 
                          gas_limit_data: Dict[str, Dict[str, int]]) -> Dict[str, any]:
        """Analyze MEV participation by operator."""
        
        validator_pubkeys = self.cache_data.get('validator_pubkeys', {})
        operator_validators = self.cache_data.get('operator_validators', {})
        
        # Map validators back to operators
        operator_mev_data = {}
        
        for operator, pubkeys in validator_pubkeys.items():
            total_validators = len(pubkeys)
            mev_registered = 0
            gas_limits = []
            
            for pubkey in pubkeys:
                # Check if this validator was found in any relay
                found_in_relay = False
                for relay_data in all_relay_data.values():
                    if pubkey in relay_data:
                        found_in_relay = True
                        break
                
                if found_in_relay:
                    mev_registered += 1
                    
                    # Get gas limit if available
                    if pubkey in gas_limit_data:
                        relay_limits = gas_limit_data[pubkey]
                        if relay_limits:
                            gas_limits.append(list(relay_limits.values())[0])
            
            # Calculate coverage and stats
            coverage_pct = (mev_registered / total_validators * 100) if total_validators > 0 else 0
            avg_gas_limit = sum(gas_limits) / len(gas_limits) if gas_limits else None
            
            operator_mev_data[operator] = {
                'display_name': self.format_operator_display(operator),
                'total_validators': total_validators,
                'mev_registered': mev_registered,
                'mev_coverage_percent': coverage_pct,
                'missing_mev': total_validators - mev_registered,
                'average_gas_limit': avg_gas_limit,
                'gas_limit_count': len(gas_limits),
                'gas_limits': gas_limits
            }
        
        return operator_mev_data
    
    def find_missing_validators(self, all_relay_data: Dict[str, Dict[str, dict]]) -> Dict[str, any]:
        """Analyze which validators are missing from MEV registrations."""
        
        all_validators = set(self._get_validator_pubkeys())
        found_validators = set()
        
        for registrations in all_relay_data.values():
            found_validators.update(registrations.keys())
        
        missing_validators = all_validators - found_validators
        
        # Analyze missing by operator
        validator_pubkeys = self.cache_data.get('validator_pubkeys', {})
        missing_by_operator = {}
        
        for operator, pubkeys in validator_pubkeys.items():
            missing_count = sum(1 for pk in pubkeys if pk in missing_validators)
            if missing_count > 0:
                total_count = len(pubkeys)
                coverage_pct = ((total_count - missing_count) / total_count * 100) if total_count > 0 else 0
                
                missing_by_operator[operator] = {
                    'display_name': self.format_operator_display(operator),
                    'missing_count': missing_count,
                    'total_count': total_count,
                    'mev_coverage_percent': coverage_pct,
                    'missing_pubkeys': [pk for pk in pubkeys if pk in missing_validators]
                }
        
        return {
            'total_missing': len(missing_validators),
            'missing_validators': list(missing_validators),
            'missing_by_operator': missing_by_operator
        }
    
    def generate_output(self, all_relay_data: Dict[str, Dict[str, dict]], 
                       analysis: Dict[str, any], missing_analysis: Dict[str, any]) -> Dict[str, any]:
        """Generate comprehensive output data."""
        
        operator_info = self._get_operator_info()
        analysis_end_time = datetime.now()
        execution_time = (analysis_end_time - self.analysis_start_time).total_seconds()
        
        output = {
            'metadata': {
                'analysis_timestamp': self.analysis_start_time.isoformat(),
                'analysis_duration_seconds': execution_time,
                'total_active_validators': len(self._get_validator_pubkeys()),
                'cache_file_used': CACHE_FILE,
                'relays_tested': len(WORKING_MEV_RELAYS),
                'working_relays': list(self.working_relays.keys()),
                'failed_relays': self.failed_relays
            },
            'summary': {
                'total_validators': len(self._get_validator_pubkeys()),
                'mev_registered_validators': analysis['total_unique_validators'],
                'mev_coverage_percentage': (analysis['total_unique_validators'] / len(self._get_validator_pubkeys()) * 100) if self._get_validator_pubkeys() else 0,
                'missing_validators': missing_analysis['total_missing'],
                'working_relays_count': len(self.working_relays),
                'gas_limit_consistency_rate': analysis['consistency_stats']['consistency_rate']
            },
            'relay_performance': {
                relay_name: {
                    'name': data['relay_display_name'],
                    'validators_found': data['validators_found'],
                    'coverage_percentage': data['coverage_percentage']
                }
                for relay_name, data in analysis['relay_coverage'].items()
            },
            'gas_limit_analysis': {
                'distribution': dict(analysis['gas_limit_distribution']),
                'consistency_stats': analysis['consistency_stats'],
                'inconsistent_validators': {
                    pubkey: limits for pubkey, limits in analysis['gas_limit_consistency'].items()
                }
            },
            'operator_analysis': analysis['operator_analysis'],
            'missing_validator_analysis': missing_analysis,
            'raw_data': {
                'relay_registrations': {
                    relay_name: {
                        'count': len(registrations),
                        'sample_pubkeys': list(registrations.keys())[:5]  # First 5 for reference
                    }
                    for relay_name, registrations in all_relay_data.items()
                },
                'operator_info': operator_info
            }
        }
        
        return output
    
    async def run_analysis(self) -> Dict[str, any]:
        """Run the complete MEV analysis."""
        
        print("🚀 STANDALONE MEV RELAY ANALYZER")
        print("=" * 60)
        print(f"📅 Analysis started: {self.analysis_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Get validator data
        validator_pubkeys = self._get_validator_pubkeys()
        if not validator_pubkeys:
            raise Exception("No active validators found in cache")
        
        print(f"📊 Analyzing {len(validator_pubkeys):,} validators across all major MEV relays")
        
        # Query all relays
        all_relay_data = await self.query_all_relays(validator_pubkeys)
        
        if not all_relay_data:
            raise Exception("No data retrieved from any relays")
        
        # Analyze results
        analysis = self.analyze_results(all_relay_data)
        missing_analysis = self.find_missing_validators(all_relay_data)
        
        # Generate output
        output = self.generate_output(all_relay_data, analysis, missing_analysis)
        
        # Print summary
        self._print_summary(output)
        
        return output
    
    def _print_summary(self, output: Dict[str, any]) -> None:
        """Print analysis summary to console."""
        
        summary = output['summary']
        
        print(f"\n🔍 ANALYSIS RESULTS SUMMARY")
        print("=" * 60)
        print(f"📊 Total Validators: {summary['total_validators']:,}")
        print(f"📈 MEV Registered: {summary['mev_registered_validators']:,}")
        print(f"📊 MEV Coverage: {summary['mev_coverage_percentage']:.1f}%")
        print(f"❓ Missing from MEV: {summary['missing_validators']:,}")
        print(f"🌐 Working Relays: {summary['working_relays_count']}")
        print(f"🔒 Consistency Rate: {summary['gas_limit_consistency_rate']:.1f}%")
        
        print(f"\n🌐 Relay Performance:")
        for relay_name, data in output['relay_performance'].items():
            print(f"   • {data['name']}: {data['validators_found']:,} validators ({data['coverage_percentage']:.1f}%)")
        
        print(f"\n⛽ Top Gas Limits:")
        gas_limits = output['gas_limit_analysis']['distribution']
        sorted_limits = sorted(gas_limits.items(), key=lambda x: x[1], reverse=True)
        for gas_limit, count in sorted_limits[:5]:
            percentage = (count / summary['mev_registered_validators'] * 100) if summary['mev_registered_validators'] > 0 else 0
            print(f"   • {gas_limit:,} gas: {count:,} validators ({percentage:.1f}%)")


def save_output(output: Dict[str, any], filename: str = OUTPUT_FILE) -> None:
    """Save analysis results to JSON file."""
    
    try:
        # Convert defaultdict to regular dict for JSON serialization
        def convert_defaultdict(obj):
            if isinstance(obj, defaultdict):
                return dict(obj)
            elif isinstance(obj, dict):
                return {k: convert_defaultdict(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_defaultdict(item) for item in obj]
            else:
                return obj
        
        output_clean = convert_defaultdict(output)
        
        with open(filename, 'w') as f:
            json.dump(output_clean, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {filename}")
        print(f"📄 Log file: {LOG_FILE}")
        
    except Exception as e:
        logging.error(f"Error saving output: {str(e)}")
        raise


async def main():
    """Main execution function."""
    
    try:
        # Create analyzer
        analyzer = StandaloneMEVAnalyzer()
        
        # Run analysis
        results = await analyzer.run_analysis()
        
        # Save results
        save_output(results)
        
        print(f"\n✅ Analysis complete!")
        
    except Exception as e:
        logging.error(f"Analysis failed: {str(e)}")
        print(f"❌ Error: {str(e)}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
