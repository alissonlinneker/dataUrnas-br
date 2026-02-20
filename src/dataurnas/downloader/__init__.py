"""Modulo de download de dados do TSE."""

from .manager import DownloadManager
from .tse_api import TSEApi

__all__ = ["DownloadManager", "TSEApi"]
