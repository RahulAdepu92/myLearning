# READ ME FIRST 
# comment s3_resource.Object(source_bucket_name,source_file_name_with_path).delete() in  the move_file
# if you are testing the lambda feature using the TEST functionality provided within Lambda editor
# or just copy the file ( to be parsed) INBOUND_FOLDER , and it will  be parsed and archived ( this is expected behavior for the production)


import json
import boto3
import re
import csv
import io
import os
import zipfile

# In a nutshell, this lambda function watches for the particular folder, as soon as file is copied , it parses the file
# uses the pre-defined the range specification file to identify the begin and end positions ,
# Upon parsing completion the file is copied in to the respective destinatinon folder , and it is deleted from the source folder
# the entire code heavily uses Lambda Environment varilable 
import logging
import logging.config
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)


  
# specifying the zip file name 

  
# opening the zip file in READ mode 

# Following constants are throughout used in the differet parts of this program

HEADER = 0
DETAIL = 1
TRAILER = 2
READ_START = 0
READ_END = 1
COLUMN_DELIMITER='|'

# End Constant Definition

def lambda_handler(event, context):
    s3_bucket_name = os.environ['INBOUND_BUCKET_NAME'] # Not required , to be commented later on , used for testing purpose.
    inbound_folder = os.environ["INBOUND_FOLDER"]
    archive_folder = os.environ['ARCHIVE_FOLDER']
    error_folder=os.environ["ERROR_FOLDER"]
    try:
        logger.info (" Entering in to Try Block")
        s3_bucket_name = event['Records'][0]['s3']['bucket']['name'] # This is the inbound bucket name
        incoming_file_name =unquote_plus(event['Records'][0]['s3']['object']['key'])
        logger.info("From Try Block : s3_bucket_name = %s, incoming_file = %s", s3_bucket_name,incoming_file)
    except:
        
        incoming_file_name=get_first_file_path(s3_bucket_name, inbound_folder)
        logger.info("From Exception Block : s3_bucket_name = %s, incoming_file = %s, inbound_folder = %s", s3_bucket_name,incoming_file_name, inbound_folder)
   
    if incoming_file_name.lower().endswith(".zip"):
        logger.info("Zip File found")
        if (unzip_file(s3_bucket_name, inbound_folder, incoming_file_name, archive_folder,error_folder)):
            logger.info("File Unzipped Successfully")
            return # Intentional Exit - 
        else:
            logger.info(" File was not unzipped successfully, no need to process any files")
            return # Because of error we exit out
    #else:
    #    if (is_it_duplicate_file(s3_bucket_name, archive_folder, incoming_file_name)): # it is called because of unzip process, so if a.zip is extracted in to a.txt then we check that there is a.zip is already out there
    #        print(" Corresponding zip file is found - This file came as a part of unzip")
    #    else:
    #        print("Regular File ")
           
        
 
    
    #This code is divided in to the three section
    #Section 1 : Read the specification file ( the position file) and create three objects ( header, detail, trailer) along with its attributes
    #Section 2 : Reads the source file ( the file that needs to be parsed), parses according to the specification ( see section 1) and write files to the
    #            folder
    #Section 3 : Archive the source file ( the file just parsed through section 2)
    
    
    s3= boto3.client('s3')  
    
    # This is a source bucket and it used throughout the code.
    s3_bucket_name=os.environ['INBOUND_BUCKET_NAME']
    print(" Bucket Name is : " + s3_bucket_name)

    # *******************SECTION 1 (Read the Specification)******************************
    # Reads the range specification
    # Range specification
    # Each line contains the start position and end positions as comma seperated arguments for example
    #  BEGIN1:END1, BEGIN2:END2, BEGIN3:END3..so on

    position_file = s3.get_object(
        Bucket=s3_bucket_name, Key=os.environ["INBOUND_FILE_SPECIFICATION"]
    )
    content = position_file["Body"].read().decode(encoding="utf-8", errors="ignore")
    all_lines = content.splitlines()
    # print(all_lines[TRAILER])
    # 3 File Objects are created and each contains its attribute

    header_file = TextFiles(
        all_lines[HEADER],
        HEADER,
        s3_bucket_name,
        os.environ["OUTBOUND_HEADER_PATH"],
        os.environ["OUTBOUND_HEADER_FILE_NAME"],
    )
    detail_file = TextFiles(
        all_lines[DETAIL],
        DETAIL,
        s3_bucket_name,
        os.environ["OUTBOUND_DETAIL_PATH"],
        os.environ["OUTBOUND_DETAIL_FILE_NAME"],
    )
    trailer_file = TextFiles(
        all_lines[TRAILER],
        TRAILER,
        s3_bucket_name,
        os.environ["OUTBOUND_TRAILER_PATH"],
        os.environ["OUTBOUND_TRAILER_FILE_NAME"],
    )

    # ************** End SECTION 1 *********************************************************************

    # *******************SECTION 2 (Read from a source file and write in to the header, detail and footer)

    # The code need to know which file it needs to parse.
    # File is just copied and we need to know the file that needs to be processed

    file_to_be_processed = get_first_file_path(s3_bucket_name, os.environ["INBOUND_FOLDER"])
    logging.info(" File to be processed : %s", file_to_be_processed)

    # get_first_file_path returns the fully qualified path
    # Lambda returns the fully qualified path so the code below extracts the file name from the qualified path (file_to_be_processed)

    seperators = file_to_be_processed.split("/")
    incoming_file_name = ""
    if len(seperators) > 0:
        incoming_file_name = seperators[len(seperators) - 1]
    logging.info("incoming file is: %s", incoming_file_name)

    # Read the file and write to the respective destination

    incoming_text = s3.get_object(
        Bucket=os.environ["INBOUND_BUCKET_NAME"], Key=file_to_be_processed
    )
    incoming_content = incoming_text["Body"].read().decode(encoding="utf-8", errors="ignore")

    # Content is split in to multiple lines, line 0 is the header line, last line is the footer line (which is all lines minus one)
    all_incoming_lines = incoming_content.splitlines()
    # print(all_incoming_lines[3])
    # Writes the header file , the file object (the second argument) contains the file path , file destination etc..
    write_file(all_incoming_lines[HEADER], header_file)

    # Writes the detail file, please note that the entire content is passed here in the first argument
    write_file(incoming_content, detail_file)

    # Writes the footer file, please note that the only footer content which is the last line (the trailer) in a big file is passed in the first argument
    write_file(all_incoming_lines[len(all_incoming_lines) - 1], trailer_file)

    # -****************END SECTION 2 *****************************************************

    # ************** Begin SECTION 3 (Copy File and Delete)****************************

    s3_archive_path = os.environ["ARCHIVE_FOLDER"] + "/" + incoming_file_name
    # print(s3_archive_path)
    move_file(s3_bucket_name, file_to_be_processed, s3_bucket_name, s3_archive_path)

    ## **************END SECTION 3


def write_file(the_content, file_type_object):
    """This method takes the content (what needs to be parsed) and where it needs to be converted and using which type of position
    definition (specified in file_type_object)  and write to a file
    it is used in the Section 2 above."""

    content_line = the_content.splitlines()
    # print(content_line)
    file_name = file_type_object.get_filepath() + file_type_object.get_filename().upper()
    reading_positions = file_type_object.get_positions()

    start_line = 0
    end_line = 1

    csvio = io.StringIO()
    file_writer = csv.writer(csvio, delimiter=COLUMN_DELIMITER)

    # If total lines of the content in a file is greater than 2 then for the detail start reading from line 1 to the last line minus one

    if len(content_line) > 2:
        start_line = 1
        end_line = len(content_line) - 1

    for line in range(start_line, end_line):
        # print(line)
        read_line = content_line[line]
        write_line = []
        for this_range in range(0, len(reading_positions)):
            write_line.append(
                read_line[
                    reading_positions[this_range][READ_START] : reading_positions[this_range][
                        READ_END
                    ]
                ]
            )

        # uncomment these three lines to add the trailng columns
        # Add current date and time - created on
        # write_line.append(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        # Add created by
        # write_line.append("lambda job")

        file_writer.writerow(write_line)
    s3_client = boto3.client("s3")
    logging.info("Creating file: %s", file_name)
    s3_client.put_object(
        Body=csvio.getvalue(),
        ContentType="text/plain",
        Bucket=file_type_object.get_bucketname(),
        Key=file_name,
    )
    csvio.close()
    del s3_client, file_writer


def return_positions(line):
    """This method returns the two dimension array of the positions
    The line contains the start position and end positions as comma seperated arguments for example
     BEGIN1:END1, BEGIN2:END2, BEGIN3:END3..so on
     This method converts such line in to two dimensional array for example
     Array = { [BEGIN1,END1],[BEGIN2, END2]..
     In a code it is accessed as Array[0][0] = BEGIN1, Array[0][1]= END1, Array[1][0]= BEGIN2, Array[1][1]=END2 and so on."""
    matrix_of_read_positions = [[]]
    range_string = line.split(",")
    logging.info("Total Elements in this lines are: %s", str(len(range_string)))
    w, h = 2, len(range_string)
    matrix_of_read_positions = [[0 for x in range(w)] for y in range(h)]
    count = 0
    for element in range_string:
        range_elements = element.split(":")
        matrix_of_read_positions[count][READ_START] = int(range_elements[READ_START])
        matrix_of_read_positions[count][READ_END] = int(range_elements[READ_END])
        count = count + 1
    # print(" Array Content")
    # print_array(matrix_of_read_positions)
    return matrix_of_read_positions


def print_array(array_pos):
    print(" Array Length is " + str(len(array_pos)))
    array_count = 0
    for array_content in range(0, len(array_pos)):
        print(" array_count" + str(array_count) + " array_content" + str(array_content))
        array_count = array_count + 1

    #   + "][" + str(0) "] = " + str(array_pos[array_count][array_content])  )


def get_first_file_path(bucket_name, bucket_path):
    """This method returns the name of the file that has been just copied in to the bucket path folder.
    This is a path  on which we would have a lambda S3 trigger."""
    # TO DO : Figure out a way if the multiple files are copied at once, this would execute the lambda function twice !!
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket_name)
    for s3_objects in bucket.objects.filter(Prefix=bucket_path):
        if s3_objects.get()["ContentType"] == "application/x-directory":
            logging.info("Directory is : %s", s3_objects.key)
        else:
            return s3_objects.key


def move_file(
    source_bucket_name,
    source_file_name_with_path,
    destination_bucket_name,
    destination_file_name_with_path,
):
    """Moves a file from source_bucket_name to the destination_bucket_name , fully qualified path needs to be specified in the source_file_name_with_path
    and destination_file_name_with destination_file_name_with_path"""
    s3_resource = boto3.resource("s3")
    s3_resource.Object(destination_bucket_name, destination_file_name_with_path).copy_from(
        CopySource={"Bucket": source_bucket_name, "Key": source_file_name_with_path}
    )
    logging.info("Now Deleting the file: %s", source_file_name_with_path)
    s3_resource.Object(source_bucket_name, source_file_name_with_path).delete()
    # comment above line if you are testing the lambda feature using the TEST functionality provided within Lambda editor


# ********* BEGIN CLASS (TextFiles) *******************************
class TextFiles:
    """TextFiles class has the following attributes
    read positions : the two dimension array of the positions
    file type : the type of the file (HEADER, DETAIL, TRAILER)
    the file path (usually set through runtime enironments)
    file name  (name of the file)"""

    def __init__(self, read_positions, file_type, bucket_name, file_path, file_name):
        self.read_positions = return_positions(read_positions)
        self.file_type = file_type
        self.file_path = file_path
        self.file_name = file_name
        self.bucket_name = bucket_name

    def get_positions(self):
        return self.read_positions

    def get_filepath(self):
        return self.file_path

    def get_filename(self):
        return self.file_name

    def get_filetype(self):
        return self.file_type

    def get_bucketname(self):
        return self.bucket_name


# ********* END CLASS (TextFiles) *******************************
# The function below unzips the file located in bucket_name\inbound_folder\incoming_zip_file_name
# It unzips to the  bucket_name\inbound_folder
# If unzip is successful then incoming_zip_file_name is archived in the bucket_name\archive_folder and True is returned.
# If unzip is not successful then incoming_zip_file_name is archived in the bucket_name\error_folder and False is returned.

def unzip_file(bucket_name: str, inbound_folder: str, incoming_zip_file_name: str , archive_folder: str, error_folder: str) -> bool: 
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    archive_path = archive_folder + "/" + incoming_zip_file_name.split("/")[-1] 
    print (" archive path = ", archive_path)
    unzip_file_successfull=True
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=incoming_zip_file_name)
        putObjects = []
        with io.BytesIO(obj["Body"].read()) as tf:
            # rewind the file
            tf.seek(0)

            # Read the file as a zipfile and process the members
            with zipfile.ZipFile(tf, mode='r') as zipf:
                
                for file in zipf.infolist():
                    fileName = file.filename
                    unzipped_file_name = inbound_folder +  "/" + fileName
                    print (" fileName: " + unzipped_file_name)
                    
                    putFile = s3.put_object(Bucket=bucket_name, Key=unzipped_file_name, Body=zipf.read(file))
                    putObjects.append(putFile)
                    print(putFile)


        # Move file and Delete zip file after unzip
        if len(putObjects) > 0:
            s3_resource.Object(bucket_name, archive_path).copy_from(CopySource = {'Bucket': bucket_name, 'Key': incoming_zip_file_name})
            deletedObj = s3.delete_object(Bucket=bucket_name, Key=incoming_zip_file_name)
            #s3.put_object(Bucket=bucket_name, Key=inbound_folder + "/ZIP") 
            print('deleted file:')
            print(deletedObj)

    except Exception as e:
        unzip_file_successfull=False
        print(e)
        print('Error getting/unzipping object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(bucket_name, incoming_zip_file_name))
        
    finally:
        if(not unzip_file_successfull):
            archive_path = error_folder + "/" + incoming_zip_file_name.split("/")[-1] #  Error Folder Path Name
            s3_resource.Object(bucket_name, archive_path).copy_from(CopySource = {'Bucket': bucket_name, 'Key': incoming_zip_file_name}) # copy to archive folder
            deletedObj = s3.delete_object(Bucket=bucket_name, Key=incoming_zip_file_name)
            print('Error Occured')
            print(deletedObj)
    return unzip_file_successfull
