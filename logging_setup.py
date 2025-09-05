from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Optional


# ============================================================================
# Module-Level Variablen
# ============================================================================

_LOGGER: Optional[logging.Logger] = None


# ============================================================================
# Setup
# ============================================================================

def setup_logging(log_dir: str, level: int = logging.INFO) -> None:
    """
    Initialisiert das Logging-System:
      - Konsole (StreamHandler)
      - Datei (Rotating-ähnlich: eine pro Tag)
    """
    global _LOGGER
    if _LOGGER:
        return  # bereits initialisiert

    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(level)

    # --- Formatter ---
    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- Console ---
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # --- File ---
    log_file = os.path.join(log_dir, f"run_{datetime.now():%Y%m%d}.log")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    _LOGGER = logger


# ============================================================================
# Zugriff auf Logger
# ============================================================================

def get_logger(name: str) -> logging.Logger:
    """
    Liefert einen benannten Logger zurück.
    setup_logging() MUSS zuvor einmal aufgerufen worden sein.
    """
    if not _LOGGER:
        raise RuntimeError("Logging wurde noch nicht initialisiert. setup_logging() zuerst aufrufen.")
    return logging.getLogger(name)
