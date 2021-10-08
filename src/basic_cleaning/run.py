#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_local_path)

    # filter outliters
    idx = df["price"].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # convert last_review to datetime
    df["last_review"] = pd.to_datetime(df["last_review"])

    # save the cleaned dataset
    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This step cleans the data")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help='Name of the input artifact',
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help='Name of the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help='Output artifact type',
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help='Description of output',
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help='Minimum price to filter price column',
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help='Maximum price to filter price column',
        required=True
    )

    args = parser.parse_args()

    go(args)
