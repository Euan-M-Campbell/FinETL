#!/usr/bin/env python3
"""Example script demonstrating FinETL usage."""

from pathlib import Path

from finetl import FinETL


def main() -> None:
    # Get the config path relative to this script
    script_dir = Path(__file__).parent
    config_path = script_dir.parent / "configs" / "example.yaml"

    print(f"Loading config from: {config_path}")

    # Create and run the ETL pipeline
    etl = FinETL.from_yaml(config_path)
    etl.run()

    print("Done! Check the ./output directory for results.")


if __name__ == "__main__":
    main()
