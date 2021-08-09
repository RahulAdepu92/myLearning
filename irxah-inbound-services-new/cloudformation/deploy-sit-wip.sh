#!/bin/bash

# this script requires jq be installed
if [ ! $( command -v jq ) ]
then
    echo "Please install jq as it is required for this script to function (brew install jq on macOS)."
    return 1
fi

# Per Environment Settings

profile="795038802291" #defined as per environment
s3Environment="dev" #defined as per environment, used in adding triggers for bucket
tagEnvironment="DEV" #defined as per environment, used in tagging resources
bucket="aws-glue-scripts-${profile}-us-east-1"  #bucket where .whl,.zip and glue scripts are ulpoaded from repo
triggerBucket="irx-accumhub-${s3Environment}-data"  #buket on which triggers are enabled
glueRole="arn:aws:iam::${profile}:role/irx-accum-glue-role" #glue execution role
connection="irx-accums-phi-rds-clr" #glue connection to access redshift db
lambdaRole="arn:aws:iam::${profile}:role/irx-accum-lambda-s3-execute-role" #lambda execution role
functionNameSuffix="_mvp2"  #distinguishes from previous run
snsEnvironment="dev" #defined as per environment, used in lambda variables
fileEnvironment="TST" #defined as per environment, used in lambda variables
secretManagerEnvironment="dv"  #defined as per environment, used in lambda variables

### Deployment Steps ###
# 1. upload artifacts
aws --profile ahub_devx s3 cp ../artifacts/  "s3://$bucket/" --recursive  
##this will copy both wheel file(consumed by glue jobs) and zip file(consumed by lambdas) to $bucket from repo artifacts folder

# 2. Upload individual glue scripts
aws --profile ahub_devx s3 cp ../glue/*.py  "s3://$bucket/glue/" 
#manually upload individual glue scripts from your local/repo

# 3. Run CloudFormation Glue scripts
aws --profile ahub_devx cloudformation deploy \
    --template-file glue.yml \
    --stack-name irx-accumhub-glue \
    --parameter-overrides profile=$profile scriptsBucket=$bucket glueRoleArn=$glueRole \
    dbconnection=$connection environment=$tagEnvironment nameSuffix=$functionNameSuffix \
    fileNameEnvironment=$fileEnvironment secretEnvironment=$secretManagerEnvironment

# 4. Run CloudFormation Lambda scripts

# a) create common layer

layer="irxah-common-py" #changes according to the run in lower environments; but will be a fixed value while prod deployment 
description="adds mvp2 code"  #changes according to the run in lower environments; but will be a fixed value while prod deployment 
aws --profile ahub_devx cloudformation deploy \
    --template-file lambda_common_layer_create_version.yml \
    --stack-name irx_accumhub_lambda_commonlayer  \
    --parameter-overrides codeBucket=$bucket layerName=$layer descriptionForVersion=$description

# b) get max layer version to use
layerVersion=$( aws --profile ahub_devx lambda list-layer-versions --layer-name irxah-common-py --max-items 1 | \
    jq '.LayerVersions[0].Version' )
layerWithVersion="arn:aws:lambda:us-east-1:${profile}:layer:irxah-common-py:$layerVersion"

# c) deploy function definition sequentially as in below 3 steps
#step 1: creates lambda functions and allows s3 buckets to trigger them by setting invoke permission resource
aws --profile ahub_devx cloudformation deploy \
    --template-file lambda_s3_trigger_step1.yml \
    --stack-name irx-accumhub-lambda-s3-trigger \
    --parameter-overrides profile=$profile codeBucket=$bucket triggerBucket=$triggerBucket \
    layerWithVersionArn=$layerWithVersion lambdaRoleArn=$lambdaRole environment=$tagEnvironment \
    nameSuffix=$functionNameSuffix snsArnEnvironment=$snsEnvironment \
    fileNameEnvironment=$fileEnvironment secretEnvironment=$secretManagerEnvironment
	
	
#step 2: execute this step when deploying lambdas for first time in any environment; this will add s3 bucket resource to the template and convert it to cft tagged;
#		 this is a one time activity and if you execute it by mistake,just an error "Buket already exists" is thrown and will not create any issues
aws --profile ahub_devx cloudformation deploy \
    --template-file lambda_s3_trigger_step2.yml \
    --stack-name irx-accumhub-lambda-s3-trigger \
    --parameter-overrides profile=$profile codeBucket=$bucket triggerBucket=$triggerBucket \
    layerWithVersionArn=$layerWithVersion lambdaRoleArn=$lambdaRole environment=$tagEnvironment \
    nameSuffix=$functionNameSuffix snsArnEnvironment=$snsEnvironment \
    fileNameEnvironment=$fileEnvironment secretEnvironment=$secretManagerEnvironment 
	
#step 3.1: execute this step to create the change set and import the bucket resource to the stack;
#          prior to execution, create $repo/resourcesToImport.txt that will contain $triggerBucket as input for resources-to-import parameter 
aws --profile ahub_devx cloudformation create-change-set \
    --template-body file://lambda_s3_trigger_step2.yml \
    --stack-name irx-accumhub-lambda-s3-trigger \
    --change-set-name ImportChangeSet \
    --change-set-type IMPORT \
    --resources-to-import file://resourcesToImport.txt \
    --parameters ParameterKey=profile,ParameterValue=$profile \
    ParameterKey=codeBucket,ParameterValue=$bucket \
    ParameterKey=triggerBucket,ParameterValue=$triggerBucket \
    ParameterKey=layerWithVersionArn,ParameterValue=$layerWithVersion \
    ParameterKey=lambdaRoleArn,ParameterValue=$lambdaRole \
    ParameterKey=environment,ParameterValue=$tagEnvironment  \
    ParameterKey=nameSuffix,ParameterValue=$functionNameSuffix \
    ParameterKey=snsArnEnvironment,ParameterValue=$snsEnvironment \
    ParameterKey=fileNameEnvironment,ParameterValue=$fileEnvironment \
    ParameterKey=secretEnvironment,ParameterValue=$secretManagerEnvironment

#step 3.2: execute the above created change set
aws --profile ahub_devx cloudformation execute-change-set --stack-name irx-accumhub-lambda-s3-trigger --change-set-name ImportChangeSet

#step 4: finally, execute this step to add s3 triggers for lambda functions
aws --profile ahub_devx cloudformation deploy \
    --template-file lambda_s3_trigger_step3.yml \
    --stack-name irx-accumhub-lambda-s3-trigger \
    --parameter-overrides profile=$profile codeBucket=$bucket triggerBucket=$triggerBucket \
    layerWithVersionArn=$layerWithVersion lambdaRoleArn=$lambdaRole \
    environment=$tagEnvironment nameSuffix=$functionNameSuffix \
    snsArnEnvironment=$snsEnvironment fileNameEnvironment=$fileEnvironment \
    secretEnvironment=$secretManagerEnvironment