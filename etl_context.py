from dataclasses import dataclass
import subprocess
import getpass
import platform


@dataclass(frozen=True)
class EtlContext:
    user_name: str
    git_hash: str
    host_name: str

    @classmethod
    def capture(cls):
        """Factory to capture current system state."""
        try:
            gh = (
                subprocess.check_output(["git", "rev-parse", "HEAD"])
                .decode("ascii")
                .strip()
            )
        except:
            gh = "unknown"

        return cls(user_name=getpass.getuser(), git_hash=gh, host_name=platform.node())
