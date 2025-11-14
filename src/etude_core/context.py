from dataclasses import dataclass
import subprocess
import getpass
import platform


@dataclass(frozen=True)
class EtlContext:
    user_name: str
    git_hash: str  # Current git commit hash
    host_name: str  # Hostname of the machine running the ETL

    @classmethod
    def capture(cls):
        """Factory method to capture the current system state."""
        try:
            gh = (
                subprocess.check_output(["git", "rev-parse", "HEAD"])
                .decode("ascii")
                .strip()
            )
        except Exception:
            gh = "unknown"

        return cls(user_name=getpass.getuser(), git_hash=gh, host_name=platform.node())
