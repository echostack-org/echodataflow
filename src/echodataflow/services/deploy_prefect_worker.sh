#!/bin/bash
# Use mamba run for non-interactive shells
mamba run -n echodataflow_20260604 prefect worker start --pool 'local'