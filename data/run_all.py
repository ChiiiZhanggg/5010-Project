import sys
import time

import pull_data
import clean_data
import build_episodes
import quality_report

STEPS = [
    ("pull_data",      pull_data.main),
    ("clean_data",     clean_data.main),
    ("build_episodes", build_episodes.main),
    ("quality_report", quality_report.main),
]


def main():
    t0 = time.perf_counter()
    for name, fn in STEPS:
        print(f"\n--- {name} ---")
        t = time.perf_counter()
        rc = fn()
        print(f"[{name}] {time.perf_counter()-t:.1f}s rc={rc}")
        if rc:
            print(f"aborting at {name}")
            return rc
    print(f"\ndone in {time.perf_counter()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
