from google.cloud import kms
from google.cloud import storage
from google.cloud import secretmanager
import base64

class CKMEKeyManager:
    def __init__(self, project_id, location):
        self.project_id = project_id
        self.location = location
        self.kms_client = kms.KeyManagementServiceClient()
        self.storage_client = storage.Client()
        self.secrets_client = secretmanager.SecretManagerServiceClient()

    def store_private_key(self, private_key: str, key_id: str, keyring_name: str):
        """Store a private key in Secret Manager and encrypt it with KMS"""
        
        # Create key ring path
        key_ring_path = self.kms_client.key_ring_path(
            self.project_id, self.location, keyring_name
        )

        # Create crypto key if it doesn't exist
        crypto_key_path = f"{key_ring_path}/cryptoKeys/{key_id}"
        try:
            crypto_key = {
                "purpose": kms.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT,
                "rotation_period": {"seconds": 2592000},  # 30 days
            }
            self.kms_client.create_crypto_key(
                request={
                    "parent": key_ring_path,
                    "crypto_key_id": key_id,
                    "crypto_key": crypto_key,
                }
            )
        except Exception as e:
            print(f"Crypto key already exists or error: {e}")

        # Create secret in Secret Manager
        secret_id = f"ckme-private-key-{key_id}"
        secret_path = f"projects/{self.project_id}/secrets/{secret_id}"
        
        try:
            self.secrets_client.create_secret(
                request={
                    "parent": f"projects/{self.project_id}",
                    "secret_id": secret_id,
                    "secret": {
                        "replication": {"automatic": {}},
                    },
                }
            )
        except Exception as e:
            print(f"Secret already exists or error: {e}")

        # Encrypt the private key
        encrypt_response = self.kms_client.encrypt(
            request={
                "name": crypto_key_path,
                "plaintext": private_key.encode("utf-8"),
            }
        )
        
        encrypted_key = base64.b64encode(encrypt_response.ciphertext).decode("utf-8")

        # Store encrypted key in Secret Manager
        self.secrets_client.add_secret_version(
            request={
                "parent": secret_path,
                "payload": {"data": encrypted_key.encode("utf-8")},
            }
        )

        return secret_path, crypto_key_path

    def retrieve_private_key(self, secret_path: str, crypto_key_path: str):
        """Retrieve and decrypt a private key"""
        
        # Get latest secret version
        secret_version = self.secrets_client.access_secret_version(
            request={"name": f"{secret_path}/versions/latest"}
        )
        
        encrypted_key = base64.b64decode(secret_version.payload.data.decode("utf-8"))

        # Decrypt the key
        decrypt_response = self.kms_client.decrypt(
            request={
                "name": crypto_key_path,
                "ciphertext": encrypted_key,
            }
        )

        return decrypt_response.plaintext.decode("utf-8")
