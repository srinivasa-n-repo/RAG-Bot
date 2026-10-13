import boto3
import mimetypes
import os

def sanitize_filename(filename):
    # Replace characters not supported by ISO-8859-1
    return filename.encode('ascii', 'ignore').decode('ascii')

def generate_presigned_url_from_s3_uri(bucket, key, expiration=3600):
    print('bucket:', bucket)
    print('key:', key)
    
    s3_client = boto3.client('s3', region_name='ap-south-1')
    
    try:
        mime_type, _ = mimetypes.guess_type(key)

        # Extract and sanitize the filename only
        original_filename = os.path.basename(key)
        safe_filename = sanitize_filename(original_filename)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': key,
                'ResponseContentType': mime_type or 'application/octet-stream',
                'ResponseContentDisposition': f'inline; filename="{safe_filename}"'
            },
            ExpiresIn=expiration
        )
        return presigned_url
    except Exception as e:
        print(f"Error generating pre-signed URL: {e}")
        return None

def generate_presigned_url(bucket_name, object_key, expiration=3600):
    s3_client = boto3.client('s3', region_name='ap-south-1')
    try:
        response = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
    except Exception as e:
        print(f"Error generating presigned URL: {e}")
        return None
    return response

def download_input_s3_document(bucket_name: str, object_key: str, download_path: str) -> str:
    """Download an S3 object to the given local path. Returns the local path."""
    if not bucket_name or not object_key:
        raise ValueError("bucket_name and object_key are required")

    # Ensure directory exists (e.g., /tmp in Lambda)
    dir_name = os.path.dirname(download_path)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    s3_client = boto3.client('s3', region_name='ap-south-1')
    try:
        s3_client.download_file(bucket_name, object_key, download_path)
        return download_path
    except Exception as e:
        print(f"Error downloading s3://{bucket_name}/{object_key} -> {download_path}: {e}")
        raise
     