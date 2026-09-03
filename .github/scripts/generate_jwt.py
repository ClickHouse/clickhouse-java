import os
import sys
import time

from jwcrypto.jwk import JWK
from jwcrypto.jwt import JWT


def get_private_key_pem(private_key_pem=None):
    """Retrieve private key PEM string from parameter or JWT_PKEY environment variable."""
    pkey = private_key_pem or os.getenv("JWT_PKEY")
    if pkey:
        return pkey

    raise ValueError(
        "Private key not found. Please set JWT_PKEY environment variable."
    )


def generate_jwt_token(
    private_key_pem=None,
    audience="ci-test-service",
    issuer="mydomain.com",
    subject="ci-test",
    expiration_seconds=300,  # Valid for 5 minutes by default
    output_file=None,
):
    """Generate a signed JWT token valid for a few minutes using the private key."""
    private_key_pem = get_private_key_pem(private_key_pem)
    jwk_key = JWK.from_pem(private_key_pem.encode("utf-8"))

    now = int(time.time())
    payload = {
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "iat": now,
        "exp": now + expiration_seconds,
    }

    header = {
        "alg": "RS256",
        "typ": "JWT",
        "kid": jwk_key.key_id,
    }

    token = JWT(header=header, claims=payload)
    token.make_signed_token(jwk_key)
    jwt_str = token.serialize()

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(jwt_str)

    return jwt_str


if __name__ == "__main__":
    out_file = sys.argv[1] if len(sys.argv) > 1 else "jwt.token"
    generate_jwt_token(output_file=out_file)
