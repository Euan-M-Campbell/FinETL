#!/usr/bin/env python3
"""Push OHLCV data to HuggingFace Hub."""

import argparse
import sys
from datetime import date

from huggingface_hub import HfApi, repo_exists
from huggingface_hub.utils import HfHubHTTPError

from finetl import FinETL

# yfinance has data from ~1962 for some stocks
EARLIEST_DATE = "1900-01-01"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract OHLCV data and push to HuggingFace Hub"
    )
    parser.add_argument(
        "--repo-id",
        required=True,
        help="HuggingFace repo ID (e.g., 'username/my-dataset')",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=["AAPL", "MSFT", "GOOGL"],
        help="Stock tickers to extract (default: AAPL MSFT GOOGL)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD). If omitted, fetches from earliest available",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD). If omitted, fetches up to today",
    )
    parser.add_argument(
        "--interval",
        default="1d",
        choices=[
            "1m",
            "2m",
            "5m",
            "15m",
            "30m",
            "60m",
            "90m",
            "1h",
            "1d",
            "5d",
            "1wk",
            "1mo",
        ],
        help="Data interval (default: 1d)",
    )
    parser.add_argument(
        "--private",
        action="store_true",
        help="Make the HuggingFace repo private",
    )
    parser.add_argument(
        "--create-repo",
        action="store_true",
        help="Create the repo if it doesn't exist",
    )
    return parser.parse_args()


def validate_hf_credentials() -> HfApi:
    """Validate HuggingFace credentials and return API client."""
    api = HfApi()
    try:
        user_info = api.whoami()
        print(f"Authenticated as: {user_info['name']}")
        return api
    except HfHubHTTPError as e:
        print(f"Error: Invalid HuggingFace credentials: {e}")
        print("Please run 'huggingface-cli login' or set HF_TOKEN environment variable")
        sys.exit(1)


def validate_repo(api: HfApi, repo_id: str, create: bool, private: bool) -> None:
    """Check if repo exists, optionally create it."""
    if repo_exists(repo_id, repo_type="dataset"):
        print(f"Repository exists: {repo_id}")
    elif create:
        print(f"Creating repository: {repo_id}")
        api.create_repo(repo_id, repo_type="dataset", private=private)
        print(f"Created {'private' if private else 'public'} repository: {repo_id}")
    else:
        print(f"Error: Repository '{repo_id}' does not exist")
        print("Use --create-repo to create it automatically")
        sys.exit(1)


def main() -> None:
    args = parse_args()

    # Validate HuggingFace credentials first
    print("Validating HuggingFace credentials...")
    api = validate_hf_credentials()

    # Check/create repository
    print(f"Checking repository: {args.repo_id}")
    validate_repo(api, args.repo_id, args.create_repo, args.private)

    # Determine date range
    # - Neither specified: fetch all available data
    # - Only start: fetch from start to today
    # - Only end: fetch from earliest to end
    # - Both: use both as specified
    start_date = args.start_date or EARLIEST_DATE
    end_date = args.end_date or date.today().isoformat()

    # Build description of what we're fetching
    if args.start_date is None and args.end_date is None:
        date_desc = "all available data"
    elif args.start_date is None:
        date_desc = f"all data up to {end_date}"
    elif args.end_date is None:
        date_desc = f"all data from {start_date} to today"
    else:
        date_desc = f"{start_date} to {end_date}"

    print(f"\nExtracting OHLCV data for: {', '.join(args.tickers)}")
    print(f"Date range: {date_desc}")
    print(f"Interval: {args.interval}")
    print(f"Destination: {args.repo_id}\n")

    config = {
        "name": "ohlcv-to-huggingface",
        "extraction": {
            "source": "yfinance",
            "tickers": args.tickers,
            "data_types": {
                "ohlcv": {
                    "enabled": True,
                    "start_date": start_date,
                    "end_date": end_date,
                    "interval": args.interval,
                }
            },
        },
        "loading": {
            "destination": "huggingface",
            "repo_id": args.repo_id,
            "private": args.private,
        },
    }

    etl = FinETL.from_dict(config)
    etl.run()

    print(
        f"\nDone! Dataset available at: https://huggingface.co/datasets/{args.repo_id}"
    )


if __name__ == "__main__":
    main()
