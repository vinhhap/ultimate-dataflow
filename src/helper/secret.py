from google.cloud import secretmanager
import json

def get_secret(secret_name: str) -> dict:
    """Access secret version based on secret name."""
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    # Access the secret version.
    response = client.access_secret_version(request={"name": secret_name})
    # Return the decoded payload.
    secret_payload = response.payload.data.decode("UTF-8")
    # Convert to dictionary
    return json.loads(str(secret_payload))