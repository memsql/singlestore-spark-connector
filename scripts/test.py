#!/usr/bin/env python
import os
import sys
DOCKERTEST_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../dockertest"))
sys.path.append(DOCKERTEST_PATH)

import pytest

if __name__ == "__main__":
    args = sys.argv[1:]

    args.append("-v")
    args.append("--pyargs")
    args += ["-c", os.path.join(DOCKERTEST_PATH, ".pytest")]

    errno = pytest.main(args)
    raise sys.exit(errno)
