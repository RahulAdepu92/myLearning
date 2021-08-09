[cmdletbinding(SupportsShouldProcess = $True)]
param(
	[Parameter(Position=1, Mandatory=$true)]
    [String]
    $targetEnvironment="sit"
)

# Settings file should be in the same folder as this script
$SETTINGS="$PSScriptRoot\settings.json"

# reads the entire settings file into an object
$Config = (Get-Content $SETTINGS -Raw) | ConvertFrom-Json

# Extract the setting that matches the environment name
# Write-Host $Config.psobject.properties.Where({$_.name -eq $targetEnvironment}).value
$EnvConfig = $Config.psobject.properties.Where({$_.name -eq $targetEnvironment}).value

# Creates environment variables for each of the keys in the environment object
foreach ($prop in $EnvConfig.psobject.properties) {
    New-Variable -Name $prop.name -Value $prop.value
}

Write-Host @"
account_Name   		 =$account_Name
account_Profile		 =$account_Profile
db_Connection  		 =$db_Connection
secret_Manager 		 =$secret_Manager
tag_Environment		 =$tag_Environment
deployment_Environment     =$deployment_Environment
security_Groups		=$security_Groups
subnet_Ids     		=$subnet_Ids
redshift_Glue_Arn   =$redshift_Glue_Arn
glue_Role_Arn  		=$glue_Role_Arn
lambda_Role_Arn		=$lambda_Role_Arn
lambda_Role_Arn_MoveToCfx = $lambda_Role_Arn_MoveToCfx
vpcSecurityGroups_For_ChangeSet		=$vpcSecurityGroups_For_ChangeSet
vpcSubnetIds_For_ChangeSet     		=$vpcSubnetIds_For_ChangeSet
tag_Region      =$tag_Region
aws_Region      =$aws_Region
sns_Stack_Name 	=$sns_Stack_Name
topic_Name1 	=$topic_Name1
topic_Name2 	=$topic_Name2
topic_Name3 	=$topic_Name3
topic_Name4 	=$topic_Name4
topic_Name5 	=$topic_Name5
topic_Name6 	=$topic_Name6
endpoint_Email 	=$endpoint_Email
data_Bucket    		=$data_Bucket
scripts_Path   		=$scripts_Path
name_Prefix         =$name_Prefix
name_Suffix 		=$name_Suffix
trigger_Bucket  	=$trigger_Bucket
enable_Trigger      =$enable_Trigger
glue_Stack_Name 	=$glue_Stack_Name
layer_Name 			=$layer_Name
lambdaLayer_Stack_Name 		=$lambdaLayer_Stack_Name
lambda_Stack_Name 			=$lambda_Stack_Name
glue_Run_Retries   = $glue_Run_Retries
rule_Status     = $rule_Status
"@

#exit;

$currenttimestamp = Get-Date -Format "MM.dd.yyyy HH:mm:ss K"
$description_For_Version = "adds latest code on ${currenttimestamp} "

Write-Host "##### Declaration of variables ends #####" -ForegroundColor DarkGreen

Write-Host "##### CFT deployment steps begins #####" -ForegroundColor DarkGreen

Write-Host "##### copying .whl, .zip & .py files to s3 path #####" -ForegroundColor DarkGreen
# 1. upload artifacts
aws --profile $account_Name s3 cp ../artifacts/  "s3://$scripts_Path/" --recursive
##copies both wheel file(consumed by glue jobs) and zip file(consumed by lambdas) to $bucket from repo artifacts folder

# 2. Upload individual glue scripts
aws --profile $account_Name s3 cp ../glue/ "s3://$scripts_Path/glue/" --recursive --exclude README.md
##copies glue scripts from your local (or) repo

# 3. Upload whl files from supplimental-libraries
aws --profile $account_Name s3 cp ../supplemental-libraries/  "s3://$scripts_Path/supplemental-libraries/" --recursive

Write-Host "##### files upload to s3 ends #####" -ForegroundColor DarkGreen

Set-PSDebug -Trace 2;

Write-Host "##### Running Glue jobs deployment #####" -ForegroundColor DarkGreen

$glue_cli_cmd =
            "aws --profile $account_Name cloudformation deploy --template-file glue.yml --stack-name $glue_Stack_Name
            --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket glueRoleArn=$glue_Role_Arn
            dbConnection=$db_Connection tagEnvironment=$tag_Environment namePrefix=$name_Prefix nameSuffix=$name_Suffix
            secretManager=$secret_Manager redshiftGlueArn=$redshift_Glue_Arn deploymentEnvironment=$deployment_Environment
            tagRegion=$tag_Region enableTrigger=$enable_Trigger glueRunRetries=$glue_Run_Retries"

Write-Host "cli command for Glue CFT deployment is:
$glue_cli_cmd" -ForegroundColor Blue

# 3. Run CloudFormation Glue script
aws --profile $account_Name cloudformation deploy --template-file glue.yml --stack-name $glue_Stack_Name `
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket glueRoleArn=$glue_Role_Arn `
    dbConnection=$db_Connection tagEnvironment=$tag_Environment namePrefix=$name_Prefix nameSuffix=$name_Suffix `
    secretManager=$secret_Manager redshiftGlueArn=$redshift_Glue_Arn deploymentEnvironment=$deployment_Environment `
    tagRegion=$tag_Region enableTrigger=$enable_Trigger glueRunRetries=$glue_Run_Retries`
##creates(if new), overrides(if existing and changed) & leaves as is(if existing and unchanged) the glue jobs that are defined in the .yml file

Write-Host "##### Glue jobs deployment ends #####" -ForegroundColor DarkGreen

echo "##### Running Lambda layer deployment #####"

$lambda_layer_cli_cmd =
        "aws --profile $account_Name cloudformation deploy --template-file lambda_common_layer_create_version.yml --stack-name $lambdaLayer_Stack_Name
        --parameter-overrides dataBucket=$data_Bucket layerName=$layer_Name descriptionForVersion=$description_For_Version"

Write-Host "cli command for Lambda Layer CFT deployment is:
$lambda_layer_cli_cmd" -ForegroundColor Blue

# 4. Run CloudFormation Lambda scripts

# a) create common layer
aws --profile $account_Name cloudformation deploy --template-file lambda_common_layer_create_version.yml --stack-name $lambdaLayer_Stack_Name `
    --parameter-overrides dataBucket=$data_Bucket layerName=$layer_Name descriptionForVersion=$description_For_Version

echo "##### Gettig the latest lambda layer version #####"

# b) get max layer version to use
$layers_Json = aws --profile $account_Name lambda list-layer-versions --layer-name $layer_Name --max-items 1 | convertfrom-json
$layer_Version = $layers_Json.LayerVersions[0].Version
$layer_With_Version_Arn = "arn:aws:lambda:${aws_Region}:${account_Profile}:layer:${layer_Name}:${layer_Version}"

Write-Host "##### Latest lambda layer in $layer_Name is $layer_With_Version_Arn #####" -ForegroundColor Blue

Write-Host "##### Running Lambda functions deployment #####"  -ForegroundColor DarkGreen

$lambda_function_cli_cmd =
    "aws --profile $account_Name cloudformation deploy --template-file lambda_s3_trigger_step1.yml --stack-name $lambda_Stack_Name
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket triggerBucket=$trigger_Bucket
    layerWithVersionArn=$layer_With_Version_Arn redshiftGlueArn=$redshift_Glue_Arn lambdaRoleArn=$lambda_Role_Arn lambdaRoleArnMoveToCfx=$lambda_Role_Arn_MoveToCfx
    deploymentEnvironment=$deployment_Environment topicName1=$topic_Name1 tagEnvironment=$tag_Environment namePrefix=$name_Prefix nameSuffix=$name_Suffix secretManager=$secret_Manager
    securityGroups=$security_Groups subnetIds=$subnet_Ids tagRegion=$tag_Region topicName4=$topic_Name4 ruleStatus=$rule_Status"

Write-Host "cli command for Lambda Function CFT deployment is:
$lambda_function_cli_cmd" -ForegroundColor Blue

###### step 1 begins ######
# c) deploy lambda function definition sequentially as explained in below 3 steps-
#step 1: creates lambda functions and allows s3 buckets to trigger them by setting invoke permission resource
aws --profile $account_Name cloudformation deploy --template-file lambda_s3_trigger_step1.yml --stack-name $lambda_Stack_Name `
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket triggerBucket=$trigger_Bucket `
    layerWithVersionArn=$layer_With_Version_Arn redshiftGlueArn=$redshift_Glue_Arn lambdaRoleArn=$lambda_Role_Arn lambdaRoleArnMoveToCfx=$lambda_Role_Arn_MoveToCfx `
    deploymentEnvironment=$deployment_Environment topicName1=$topic_Name1 tagEnvironment=$tag_Environment namePrefix=$name_Prefix nameSuffix=$name_Suffix secretManager=$secret_Manager `
    securityGroups=$security_Groups subnetIds=$subnet_Ids tagRegion=$tag_Region topicName4=$topic_Name4 ruleStatus=$rule_Status `
###### step 1 ends ######

Write-Host "##### Lambda functions creation ends #####" -ForegroundColor DarkGreen

###### step 2 begins ######

Write-Host "##### creating change set to add s3 bucket now #####" -ForegroundColor DarkGreen

#step 2.1: execute this step to create the change set and import the existing ${trigger_Bucket} resource to the stack;
aws --profile $account_Name cloudformation create-change-set --template-body file://lambda_s3_trigger_step2.yml --stack-name $lambda_Stack_Name `
    --change-set-name ImportChangeSet `
    --change-set-type IMPORT `
    --resources-to-import "ResourceType=AWS::S3::Bucket,LogicalResourceId=Bucket,ResourceIdentifier={BucketName=${trigger_Bucket}}" `
    --parameters ParameterKey=accountProfile,ParameterValue=$account_Profile ParameterKey=scriptsPath,ParameterValue=$scripts_Path `
    ParameterKey=dataBucket,ParameterValue=$data_Bucket ParameterKey=triggerBucket,ParameterValue=$trigger_Bucket `
    ParameterKey=layerWithVersionArn,ParameterValue=$layer_With_Version_Arn ParameterKey=redshiftGlueArn,ParameterValue=$redshift_Glue_Arn `
    ParameterKey=lambdaRoleArn,ParameterValue=$lambda_Role_Arn ParameterKey=lambdaRoleArnMoveToCfx,ParameterValue=$lambda_Role_Arn_MoveToCfx `
    ParameterKey=topicName1,ParameterValue=$topic_Name1 ParameterKey=tagEnvironment,ParameterValue=$tag_Environment ParameterKey=namePrefix,ParameterValue=$name_Prefix ParameterKey=nameSuffix,ParameterValue=$name_Suffix `
    ParameterKey=deploymentEnvironment,ParameterValue=$deployment_Environment ParameterKey=secretManager,ParameterValue=$secret_Manager `
    ParameterKey=securityGroups,ParameterValue=$vpcSecurityGroups_For_ChangeSet ParameterKey=subnetIds,ParameterValue=$vpcSubnetIds_For_ChangeSet `
    ParameterKey=tagRegion,ParameterValue=$tag_Region ParameterKey=topicName4,ParameterValue=$topic_Name4 ParameterKey=ruleStatus,ParameterValue=$rule_Status `

echo "##### waits for maximum 45 seconds of time to refresh the event #####"

Start-Sleep -s 45

echo "##### executing change set now #####"

#step 2.2: execute the above created change set
aws --profile $account_Name cloudformation execute-change-set --stack-name $lambda_Stack_Name --change-set-name ImportChangeSet
###### step 2 ends ######

echo "##### waits for maximum 45 seconds of time to refresh the event #####"

Start-Sleep -s 45

###### step 3 begins ######
#step 3: finally, execute this step to add s3 triggers for lambda functions
aws --profile $account_Name cloudformation deploy --template-file lambda_s3_trigger_step3.yml --stack-name $lambda_Stack_Name `
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket triggerBucket=$trigger_Bucket `
    layerWithVersionArn=$layer_With_Version_Arn redshiftGlueArn=$redshift_Glue_Arn lambdaRoleArn=$lambda_Role_Arn lambdaRoleArnMoveToCfx=$lambda_Role_Arn_MoveToCfx `
    deploymentEnvironment=$deployment_Environment topicName1=$topic_Name1 tagEnvironment=$tag_Environment namePrefix=$name_Prefix nameSuffix=$name_Suffix secretManager=$secret_Manager `
    securityGroups=$security_Groups subnetIds=$subnet_Ids tagRegion=$tag_Region topicName4=$topic_Name4 ruleStatus=$rule_Status `
###### step 3 ends ######

Write-Host "##### Lambda functions deployment and addition of triggers ends #####" -ForegroundColor DarkGreen

Write-Host "##### Running SNS topics deployment #####" -ForegroundColor DarkGreen

$sns_cli_cmd =
    "aws --profile $account_Name cloudformation deploy --template-file sns.yml --stack-name $sns_Stack_Name
    --parameter-overrides topicName1=$topic_Name1 topicName2=$topic_Name2 topicName3=$topic_Name3 topicName4=$topic_Name4 topicName5=$topic_Name5 endpointEmail=$endpoint_Email topicName6=$topic_Name6 endpointEmail=$endpoint_Email"

Write-Host "cli command for SNS CFT deployment is:
$sns_cli_cmd" -ForegroundColor Blue

# 5. Run CloudFormation SNS script
##deploys below 2 SNS topics and adds a subscription in the environment.
##you can add the new subscriptions to the created 2 topics by modifying ${endpoint_Email} and rerunning the below cli command
aws --profile $account_Name cloudformation deploy --template-file sns.yml --stack-name $sns_Stack_Name `
    --parameter-overrides topicName1=$topic_Name1 topicName2=$topic_Name2 topicName3=$topic_Name3 topicName4=$topic_Name4 topicName5=$topic_Name5 endpointEmail=$endpoint_Email topicName6=$topic_Name6 endpointEmail=$endpoint_Email

Write-Host "*** Note: if above SNS stack updation got failed, ignore the error as there might be an already created SNS topic manually in the environment #####" -ForegroundColor Magenta

Write-Host "##### SNS topics deployment ends #####" -ForegroundColor DarkGreen

Write-Host "##### Ahub Deployment Ends #####" -ForegroundColor DarkGreen



##### PS variable section ######
##Below are the powershell variables just for refernce that may be helpful to deploy individual developing code and unit test.
# Nothing to do with current script and that is why it is commented.
	
<#
$account_Name = "ahub_sitx"
$account_Profile" : "795038802291"
$db_Connection = "irx-accums-phi-rds-clr"
$secret_Manager = "irx-ahub-dv-redshift-cluster"
$tag_Environment = "DEV"
$file_Name_Environment = "TST"
$deployment_Environment = "SIT"
$acchist_Out_File = "ACCHIST_TSTLD_BCIINRX_"
$file_Type = "T"
$cfx_Bci_Bucket = "irx-nonprod-bci-east-1-sftp-app-outbound"
$cfx_Bci_Folder = "sit/accums/"
$cfx_Cvs_Bucket = "irx-nonprod-cvs-east-1-sftp-app-outbound"
$skip_Delivery = "FALSE"
$security_Groups = "sg-0f181993547c48186 sg-0411ae853409470fd sg-077f4be9869df056a"
$subnet_Ids = "subnet-021888688bf4705c0 subnet-01460daa36a0b1381"
$redshift_Glue_Arn = "arn:aws:iam::795038802291:role/irx-accum-phi-redshift-glue"
$glue_Role_Arn = "arn:aws:iam::795038802291:role/irx-accum-glue-role"
$lambda_Role_Arn = "arn:aws:iam::795038802291:role/irx-accum-lambda-s3-execute-role"
$lambda_Role_Arn_MoveToCfx = "arn:aws:iam::795038802291:role/irx-accum-lambda-s3-role"
$sns_Arn = "arn:aws:sns:us-east-1:795038802291:AHUB_EMAIL_NOTIF"
$vpcSecurityGroups_For_ChangeSet = "sg-0f181993547c48186\\sg-0411ae853409470fd\\sg-077f4be9869df056a"
$vpcSubnetIds_For_ChangeSet = "subnet-021888688bf4705c0\\subnet-01460daa36a0b1381"
$sns_Stack_Name = "irx-accumhub-sns"
$topic_Name1 = "AHUB_EMAIL_NOTIF"
$topic_Name2 = "irx_ahub_error_notification"
$endpoint_Email = "test@email.com"
$data_Bucket = "irx-accumhub-dev-data"
$scripts_Path = "irx-accumhub-dev-data/deployment"
$name_Suffix = "_mvp2"
$trigger_Bucket = "irx-accumhub-dev-data"
$glue_Stack_Name = "irx-accumhub-glue"
$layer_Name = "irxah-common-py"
$lambdaLayer_Stack_Name = "irx-accumhub-lambda-commonlayer"
$lambda_Stack_Name = "irx-accumhub-lambda-s3-trigger"
#>

##below command is just for future refernce. Can be made available whenever required.
<#
#step 2.1: execute this step only if you wish to deploy a new s3 bucket for first time in the environment through cft;
#		   however we have s3 bucket created already in the environment on which triggers will be added, so no need to execute this command and that is why it is commented;
#		   if you need a test bucket for testing triggers, uncomment this step and a new bucket will be created out of this step automatically;
#		   this is a one time activity(need not run this step everytime you rerun the stack) and if you execute it by mistake,just an error "Buket already exists" is thrown and #			will not create any issues;
aws --profile $account_Name cloudformation deploy `
    --template-file lambda_s3_trigger_step2.yml `
    --stack-name $lambda_Stack_Name `
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket triggerBucket=$trigger_Bucket layerWithVersionArn=$layer_With_Version_Arn redshiftGlueArn=$redshift_Glue_Arn lambdaRoleArn=$lambda_Role_Arn lambdaRoleArnMoveToCfx=$lambda_Role_Arn_MoveToCfx snsArn=$sns_Arn
	tagEnvironment=$tag_Environment nameSuffix=$name_Suffix secretManager=$secret_Manager fileNameEnvironment=$file_Name_Environment deploymentEnvironment=$deployment_Environment acchistOutFile=$acchist_Out_File cfxBciBucket=$cfx_Bci_Bucket cfxBciFolder=$cfx_Bci_Folder cfxCvsBucket=$cfx_Cvs_Bucket skipDelivery=$skip_Delivery fileType=$file_Type securityGroups=$security_Groups subnetIds=$subnet_Ids
#>
