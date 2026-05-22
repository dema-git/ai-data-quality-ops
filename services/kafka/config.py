##############################################################
#
# services/kafka/config.py
#
# Kafka connection and topic settings.
#
# This configuration class defines the essential parameters for connecting
# to a Kafka cluster and specifying topic names for message streaming.
##############################################################

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class KafkaConfig:
    """
    Kafka connection and topic settings
    """
    bootstrap_servers: Sequence[str] = ("kafka:9092",)
    sessions_topic: str = "sessions_raw"
