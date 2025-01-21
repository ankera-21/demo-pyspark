from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import google.auth
from google.cloud import kms_v1, storage, secretmanager_v1
import base64
import cryptography.hazmat.primitives.asymmetric.rsa as rsa
import cryptography.hazmat.primitives.asymmetric.padding as padding
import cryptography.hazmat.primitives.hashes as hashes
from cryptography.hazmat.primitives import serialization

# Step 1: Fetch the encrypted PEM file securely
def get_encrypted_pem_from_gcs(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return blob.download_as_bytes()

# Step 2: Decrypt PEM content using Google Cloud KMS
def decrypt_pem_with_kms(encrypted_pem, project_id, location, key_ring, key_name):
    kms_client = kms_v1.KeyManagementServiceClient()
    key_path = kms_client.crypto_key_path(project_id, location, key_ring, key_name)
    decrypt_response = kms_client.decrypt(request={"name": key_path, "ciphertext": encrypted_pem})
    return decrypt_response.plaintext

# Step 3: Load the PEM content as an RSA private key
def load_private_key(pem_content):
    private_key = serialization.load_pem_private_key(
        pem_content,
        password=None,
    )
    return private_key

# Step 4: Define a UDF for decryption
def decrypt_udf(encrypted_value, private_key):
    decrypted_value = private_key.decrypt(
        base64.b64decode(encrypted_value),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    return decrypted_value.decode('utf-8')

# Main Dataproc Job Logic
def main():
    # Parameters
    bucket_name = "your-gcs-bucket"
    encrypted_pem_file = "path/to/encrypted_pem_file"
    project_id = "your-project-id"
    location = "kms-location"
    key_ring = "your-key-ring"
    key_name = "your-kms-key"

    # Fetch and decrypt the PEM file
    encrypted_pem = get_encrypted_pem_from_gcs(bucket_name, encrypted_pem_file)
    pem_content = decrypt_pem_with_kms(encrypted_pem, project_id, location, key_ring, key_name)
    private_key = load_private_key(pem_content)

    # Initialize Spark session
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Secure PEM Decryption").getOrCreate()

    # Read the encrypted DataFrame
    df = spark.read.format("parquet").load("path/to/encrypted/dataframe")

    # Register UDF for decryption
    decrypt = udf(lambda x: decrypt_udf(x, private_key), StringType())
    df_decrypted = df.withColumn("decrypted_column", decrypt(df["encrypted_column"]))

    # Show or save the decrypted data
    df_decrypted.show()

# Trigger the main function
if __name__ == "__main__":
    main()
