"""Entry point for `python -m servitor_workflows`."""
from .cli import main

import sys

if __name__ == "__main__":
    sys.exit(main())
