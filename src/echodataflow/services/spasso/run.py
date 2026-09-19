"""Run the SPASSO visualization service."""

from __future__ import annotations

import argparse
from pathlib import Path

import geoviews as gv
import holoviews as hv
import panel as pn

from echodataflow.services.spasso.app import create_spasso_app
from echodataflow.services.spasso.config import load_spasso_viz_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the SPASSO visualization dashboard."
    )

    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Path to the SPASSO visualization YAML configuration.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = load_spasso_viz_config(args.config)

    hv.extension("bokeh")
    gv.extension("bokeh")
    pn.extension(
        loading_indicator=True,
        loading_spinner="dots",
    )

    app = lambda: create_spasso_app(config)

    pn.serve(
        {"spasso": app},
        port=1804,
        websocket_origin="*",
        admin=True,
        show=False,
        autoreload=False,
        keep_alive=40000,
        check_unused_sessions_milliseconds=30000,
    )


if __name__ == "__main__":
    main()