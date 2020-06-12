# 
# PURPOSE : 
# It has two purpose 
# 1. Copies the cvs file located within the Accum Hub controlled bucket ( such as irx-accumhub-dev-data) and 
#    folder ( such as outbound/cvs/) to CFX controlled bucket (such as irx-nonprod-cvs-east-1-sftp-app-outbound) and folder
#    ( sit/accums/).
# 2. Copy whatever that has been copied , make a safe copy of the same in the archive folder defined by ARCHIVE_PATH. 
#    and then delete the file. Basically this is more or less cut and paste feature.

# This code utiizes the environment varilables which are explained as follows : 
#  SOURCE_BUCKET_ACCUMHUB  : It is Accum Hub S3 bucket name, call it a source bucket from where we will copy the required file.
#  SOURCE_OUTBOUND_FOLDER_ACCUMHUB : It is Accum Hub outbound folder located within SOURCE_BUCKET_ACCUMHUB  from where the file will be copied. Please note
# that it is assumed that SOURCE_OUTBOUND_FOLDER_ACCUMHUB resides in the SOURCE_BUCKET_ACCUMHUB. 

#  DESTINATION_BUCKET_CFX  : It is CFX S3 bucket name, call it a destination bucket to  which we will copy the required file from Source bucket.
#  DESTINATION_OUTBOUND_FOLDER_CFX: It is CFX   outbound folder located within  DESTINATION_BUCKET_CFX  to which  the file will be copied. Please note
# that it is assumed that DESTINATION_OUTBOUND_FOLDER_CFX resides in the DESTINATION_OUTBOUND_FOLDER_CFX. 

#  ARCHIVE_PATH : The folder path within the Accum Hub controlled bucket ( as defined in the SOURCE_BUCKET_ACCUMHUB )
#  

# In most simplest term it can be described as follows : 
# For Each .txt  file found in SOURCE_BUCKET_ACCUMHUB\SOURCE_OUTBOUND_FOLDER_ACCUMHUB 
#       BEGIN 
#           1. COPY SOURCE_BUCKET_ACCUMHUB\SOURCE_OUTBOUND_FOLDER_ACCUMHUB\filename.txt to DESTINATION_BUCKET_CFX\DESTINATION_OUTBOUND_FOLDER_CFX
#           2. COPY SOURCE_BUCKET_ACCUMHUB\SOURCE_OUTBOUND_FOLDER_ACCUMHUB\filename.txt t0 SOURCE_BUCKET_ACCUMHUB\ARCHIVE_PATH
#           3. DELETE filename.txt
#       END



import json
import boto3
import os
from io import BytesIO, StringIO
import zipfile


def lambda_handler(event, context):
    s3_source_accum = boto3.resource('s3')
    s3_accum = boto3.client('s3')
    
    #assign the source bucket to s3_bucket_name
    s3_source_bucket_name_accum = os.environ['SOURCE_BUCKET_ACCUMHUB']
    
    #asign the destination bucket to s3_cfx_bucket_name
    s3_destionation_bucket_cfx = os.environ['DESTINATION_BUCKET_CFX']
    #print ('Copy Started')

    s3_source_bucket_accum = s3_source_accum.Bucket(s3_source_bucket_name_accum)
   
    # for loop below would only run if there is a .txt file in the SOURCE_OUTBOUND_FOLDER_ACCUMHUB
    for key in s3_source_bucket_accum.objects.filter(Prefix=os.environ['SOURCE_OUTBOUND_FOLDER_ACCUMHUB'], Delimiter='/'):
        if str(key).lower().endswith(".txt"):
            # In the code below, key.key represents the file name and it will be fully qualified path
            archive_source_accum = {'Bucket': s3_source_bucket_name_accum, 'Key': (key.key)}
            archive_destination_accum_file = os.environ['ARCHIVE_PATH'] + os.path.basename(key.key)
            # We will generate the file directly in to ARCHIVE PATH folder and then give it to CFX 
            
            zip_file_name = os.path.basename(key.key).replace(".TXT",".ZIP") # If file is A.TXT , it becomes A.zip
            fully_qualified_zip_file_path= zip_text_file(s3_source_bucket_name_accum, key.key,  os.environ['ARCHIVE_PATH'])
            archive_source_accum_zip = {'Bucket': s3_source_bucket_name_accum, 'Key': (fully_qualified_zip_file_path)}
                
            # Above gets you the zip file and fully qualified zip file path    
            
            
            destination_cfx_file = os.environ['DESTINATION_OUTBOUND_FOLDER_CFX'] + zip_file_name ## Eearlier it was text file os.path.basename(key.key)
            
            s3_resource = boto3.resource('s3')
            print('s3_destionation_bucket_cfx = ' , s3_destionation_bucket_cfx)
            print ('destination_cfx_file = ' , destination_cfx_file)
            print ('archive_source_accum_zip ( Copied  = ' , archive_source_accum_zip)
            print ('fully_qualified_zip_file_path = ' , fully_qualified_zip_file_path)
            s3_accum.copy_object(ACL='bucket-owner-full-control', Bucket=s3_destionation_bucket_cfx, Key=destination_cfx_file, CopySource = archive_source_accum_zip)
            
            
            #copy the .txt file to archive folder ( As descibed step 2 above)
            s3_accum.copy_object( Bucket=s3_source_bucket_name_accum, Key=archive_destination_accum_file, CopySource = archive_source_accum)
 
            #Delete the .txt file from source bucket under history folder ( as described step 3 above)
            # here key.key represents file name.
            s3_accum.delete_object(Bucket=s3_source_bucket_name_accum, Key=key.key)



def compress(body,key):
    data_io =BytesIO()
    zf = zipfile.ZipFile(data_io, 'w')
    zf.writestr(key,body, compress_type=zipfile.ZIP_DEFLATED) 
    zf.close()
    data_io.seek(0)
    return data_io.read()

def zip_text_file(s3_bucket_name, text_file_that_needs_to_be_zipped,  zip_folder):
    s3 = boto3.client('s3')
    body=s3.get_object(Bucket=s3_bucket_name, Key= text_file_that_needs_to_be_zipped)['Body'].read()
    file_name_inside_zip = os.path.basename(text_file_that_needs_to_be_zipped) ## if path is a/b/c.txt then returns c.txt
    returnData=compress(body,file_name_inside_zip) # Returns compressed data 
    
    zip_file_name=file_name_inside_zip.replace(".TXT",".ZIP") ## Corresponding zip file name , if it is A.TXT we generate A.ZIP
    
    fully_qualified_zip_file_path = zip_folder +  zip_file_name
    s3.put_object(Bucket=s3_bucket_name, Key=fully_qualified_zip_file_path, Body=returnData) # Writes compressed data
    return fully_qualified_zip_file_path