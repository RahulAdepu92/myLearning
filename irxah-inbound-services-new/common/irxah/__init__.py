import os
import sys

if "PRETTY_EXCEPTIONS" in os.environ or "--prettyExceptions" in sys.argv:
    try:
        from better_exceptions import hook

        hook()
        print("Installed better_exceptions")
    except Exception:
        print("Failed to import better_exceptions.")
        try:
            from frosch import hook

            hook()
            print("Installed frosch")
        except Exception:
            print("Failed to import frosch, too.")