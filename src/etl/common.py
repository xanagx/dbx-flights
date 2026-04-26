from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import yaml
from pyspark.sql import SparkSession


@dataclass
class PipelineConfig:
    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    raw_paths: dict[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="conf/pipeline_config.yaml",
        help="Path to pipeline YAML config.",
    )
    return parser.parse_args()


def load_config(path: str) -> PipelineConfig:
    candidate = Path(path)
    if not candidate.exists():
        project_root = Path(__file__).resolve().parents[2]
        candidate = (project_root / path).resolve()
    with candidate.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return PipelineConfig(**data)


def get_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def fq_schema(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}"
