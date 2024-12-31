import boto3


def get_aws_s3(
    aws_access_key: str,
    aws_security_token: str,
    bucket_path: str,
    prefix_path: str,
    import_path: str,
    import_file: str
) -> str:
    try:
        AWS_ACCESS_KEY = aws_access_key
        AWS_SECURITY_TOKEN = aws_security_token
        BUCKET_PATH = bucket_path
        PREFIX_PATH = prefix_path

        FULL_FILE_NAME = import_file
        FULL_IMPORT_PATH = import_path + "/" + FULL_FILE_NAME

        # Connection Established.
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECURITY_TOKEN,
        )

        # Downloading file from S3 Bucket
        s3.download_file(BUCKET_PATH, PREFIX_PATH, FULL_IMPORT_PATH)

        s3.close()

        return f"{FULL_IMPORT_PATH}"
    except Exception as e:
        return f"Error: {e}"
