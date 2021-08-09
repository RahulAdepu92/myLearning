import io
import os
import zipfile
import boto3
from .s3 import read_file


s3_client = boto3.client("s3")


def compress_text(content: str, name_of_file_inside_archive: str) -> bytes:
    """Compresses content.
    :param content: the text to be compressed.
    :param name_of_file_inside_archive: what the content is to be named inside archive
    :return: the content of the zip file as bytes."""
    data_io = io.BytesIO()
    zf = zipfile.ZipFile(data_io, "w")
    zf.writestr(name_of_file_inside_archive, content, compress_type=zipfile.ZIP_DEFLATED)
    zf.close()
    data_io.seek(0)
    return data_io.read()


def zip_text_file(s3_bucket_name: str, text_file_to_archive: str, zip_folder: str) -> str:
    """Compresses an S3 file to a zip file with the same name, but zip extension.
    :param text_file_to_archive: .txt file which has to be archived. Here, outbound/txtfiles
    :return : path of zip file."""
    body: str = read_file(s3_bucket_name, text_file_to_archive)
    ## if path is a/b/c.txt then returns c.txt
    file_name_inside_zip = os.path.basename(text_file_to_archive)
    zipped_data = compress_text(body, file_name_inside_zip)  # Returns compressed data

    ## Corresponding zip file name , if it is A.TXT we generate A.ZIP
    zip_file_name = file_name_inside_zip[0:-4] + ".ZIP"

    # write compressed data to zip folder
    fully_qualified_zip_file_path = os.path.join(zip_folder, zip_file_name)
    s3_client.put_object(Bucket=s3_bucket_name, Key=fully_qualified_zip_file_path, Body=zipped_data)
    return fully_qualified_zip_file_path
