# NodeSet Validator Tracker

A monitoring tool for tracking NodeSet protocol validators on Ethereum. Analyzes blockchain transactions to identify validator deposits, track exits, and provide operator statistics.

## Features

- **Validator Discovery**: Identifies NodeSet validator deposits from blockchain data
- **Exit Tracking**: Monitors validator exits via beacon chain API
- **Operator Analytics**: Detailed statistics and concentration metrics
- **Incremental Processing**: Efficient caching for fast updates

## Installation

1. Clone and install:
```bash
git clone https://github.com/steelyninja/nodeset-validator-tracker.git
cd nodeset-validator-tracker
pip install -r requirements.txt
```

2. Configure environment:
```bash
export ETH_CLIENT_URL="http://your-ethereum-node:8545"
export BEACON_API_URL="http://your-beacon-api:5052"
export ETHERSCAN_API_KEY='your api key'
```

## Usage

```bash
python nodeset_validator_tracker.py
```

### Example Output

```
=== ACTIVE VALIDATOR DISTRIBUTION ===
Operators with 5 active validators: 29
Operators with 6 active validators: 50
Operators with 7 active validators: 45

Active validators: 791
Exited validators: 9
Maximum operator concentration: 0.0101
```

## Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| `ETH_CLIENT_URL` | Ethereum execution client endpoint | Yes |
| `BEACON_API_URL` | Beacon chain API endpoint | Yes |
| `ETHERSCAN_API_KEY` | Etherscan API Key | Yes |

## Requirements

- Python 3.8+
- Ethereum execution client (Geth, Erigon, etc.)
- Beacon chain API (Lighthouse, Prysm, Teku, Nimbus)
- Etherscan API Key

## How It Works

1. **Scans** Ethereum blocks for beacon deposits in NodeSet multicall transactions
2. **Tracks** validator lifecycle from deposit through activation to exit
3. **Monitors** exit status via beacon chain API
4. **Caches** results for efficient incremental updates

## Troubleshooting

- **Connection errors**: Verify Ethereum client is running and accessible
- **Performance issues**: Use local clients for best performance
- **Exit tracking disabled**: Check beacon API connectivity

Logs are written to `nodeset_validator_tracker.log` for detailed debugging.

## License

MIT License - see [LICENSE](LICENSE) file for details.

