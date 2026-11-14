"""Generate RS256 keypair for JWT signing (dev only).

Usage:
    poetry run python scripts/gen_keys.py
    poetry run python scripts/gen_keys.py --force

Writes:
    keys/jwt_private.pem  (PKCS#8, no password)
    keys/jwt_public.pem   (SubjectPublicKeyInfo)

Refuses to overwrite existing keys unless --force is passed.
"""

import argparse
import sys
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

KEYS_DIR = Path(__file__).resolve().parent.parent / "keys"
PRIVATE_PATH = KEYS_DIR / "jwt_private.pem"
PUBLIC_PATH = KEYS_DIR / "jwt_public.pem"


def generate(force: bool) -> None:
    if (PRIVATE_PATH.exists() or PUBLIC_PATH.exists()) and not force:
        print(
            f"Keys already exist in {KEYS_DIR}. "
            "Refusing to overwrite. Use --force to regenerate.",
            file=sys.stderr,
        )
        sys.exit(1)

    KEYS_DIR.mkdir(parents=True, exist_ok=True)

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    PRIVATE_PATH.write_bytes(private_pem)
    PUBLIC_PATH.write_bytes(public_pem)

    try:
        PRIVATE_PATH.chmod(0o600)
        PUBLIC_PATH.chmod(0o644)
    except OSError:
        # Windows / filesystems without POSIX permissions — ignore.
        pass

    print(f"Generated {PRIVATE_PATH}")
    print(f"Generated {PUBLIC_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate RS256 JWT keypair")
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing keys",
    )
    args = parser.parse_args()
    generate(force=args.force)
