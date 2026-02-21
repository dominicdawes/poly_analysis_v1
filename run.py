#!/usr/bin/env python3
"""
run.py
------
Main entry point for poly-analysis-v1.

Execution order
---------------
1. Run scripts/health_check.py as a subprocess.
2. Abort if health check returns exit code != 0.
3. Initialise DB, services, Flask app.
4. Start the ingestion service (background threads).
5. Start Flask (blocking, main thread).

Usage
-----
    python run.py
"""

import logging
import os
import subprocess
import sys
import time

# ----------------------------------------------------------------
# Ensure project root is importable
# ----------------------------------------------------------------
_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


def _setup_logging(logs_dir: str, debug: bool = False) -> None:
    os.makedirs(logs_dir, exist_ok=True)
    level = logging.DEBUG if debug else logging.INFO
    fmt   = "%(asctime)s  %(name)-24s %(levelname)-8s %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(logs_dir, "app.log"), encoding="utf-8"),
    ]
    logging.basicConfig(level=level, format=fmt, handlers=handlers)


def _run_health_check() -> int:
    """Invoke health_check.py as a subprocess; return its exit code."""
    script = os.path.join(_ROOT, "scripts", "health_check.py")
    result = subprocess.run([sys.executable, script])
    return result.returncode


def _banner(msg: str) -> None:
    width = 60
    print("\n" + "=" * width)
    print(f"  {msg}")
    print("=" * width + "\n")


# ----------------------------------------------------------------
# Main
# ----------------------------------------------------------------
def main() -> None:
    # Minimal bootstrap so we can read LOGS_DIR / FLASK_DEBUG before
    # the full Config object is ready.
    from dotenv import load_dotenv
    load_dotenv()

    logs_dir = os.getenv("LOGS_DIR", "logs")
    debug    = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    _setup_logging(logs_dir, debug)

    logger = logging.getLogger("run")

    _banner("Poly Analysis v1 — Starting up")

    # ---- Health check --------------------------------------------------
    print("Running pre-flight health check…\n")
    exit_code = _run_health_check()

    if exit_code != 0:
        print("\n❌  Health check failed — fix the issues above and try again.\n")
        sys.exit(1)

    print("\n✅  Health check passed — initialising services…\n")

    # ---- Config --------------------------------------------------------
    from conf.config import Config
    config = Config.from_env()
    errors = config.validate()
    if errors:
        for err in errors:
            logger.error("Config error: %s", err)
        sys.exit(1)

    logger.info("Config loaded: %s", config)

    # ---- Storage -------------------------------------------------------
    os.makedirs(config.output_dir, exist_ok=True)
    os.makedirs(config.logs_dir,   exist_ok=True)

    from db import Database
    db = Database(config.db_path)
    logger.info("Database ready: %s", config.db_path)

    # ---- Services ------------------------------------------------------
    from services.analysis  import AnalysisService
    from services.ingestion import IngestionService

    analysis  = AnalysisService(db, config.whale_threshold)
    ingestion = IngestionService(config, db)

    # ---- Flask app -----------------------------------------------------
    from app.app import create_app
    app = create_app(config, db, analysis, ingestion=ingestion)

    # ---- Start ingestion (background) ----------------------------------
    ingestion.start()
    logger.info("Ingestion service started — market=%s", config.market_id)

    # Allow the first poll to complete before serving the UI
    time.sleep(2)

    # ---- Start Flask (blocks until Ctrl-C) -----------------------------
    url = f"http://{'localhost' if config.flask_host == '0.0.0.0' else config.flask_host}:{config.flask_port}"
    print(f"\n  Dashboard → {url}\n")
    logger.info("Starting Flask on %s:%d", config.flask_host, config.flask_port)

    app.run(
        host=config.flask_host,
        port=config.flask_port,
        debug=config.flask_debug,
        use_reloader=False,   # reloader would duplicate ingestion threads
    )


if __name__ == "__main__":
    main()
