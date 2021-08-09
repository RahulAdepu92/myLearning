Here are three validation approaches, with the pros and cons of each described in their individual files
(look towards the end of the file).

To run:

# Naive approach

`python .\main_naive.py .\file_badstructure.txt ; python .\main_naive.py .\file_baddata.txt`

# Method configuration

` python .\main_method_config.py .\file_badstructure.txt ; python .\main_method_config.py .\file_baddata.txt`

# External configuration

`python .\main_external_config.py .\file_badstructure.txt .\validations.config.json ; python .\main_external_config.py .\file_baddata.txt .\validations.config.json`