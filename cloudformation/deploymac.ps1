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
account_Name   		=$account_Name
account_Profile		=$account_Profile
db_Connection  		=$db_Connection
secret_Manager 		=$secret_Manager
tag_Environment		=$tag_Environment
file_Name_Environment=$file_Name_Environment
cfx_Environment		=$cfx_Environment
sns_Environment     =$sns_Environment
acchist_Environment =$acchist_Environment
file_Type			=$file_Type
cfx_Bci_Bucket		=$cfx_Bci_Bucket
cfx_Cvs_Bucket		=$cfx_Cvs_Bucket
skip_Delivery		=$skip_Delivery
security_Groups		=$security_Groups
subnet_Ids     		=$subnet_Ids
glue_Role_Arn  		=$glue_Role_Arn
lambda_Role_Arn		=$lambda_Role_Arn
lambda_Role_Arn_MoveToCfx = $lambda_Role_Arn_MoveToCfx
vpcSecurityGroups_For_ChangeSet		=$vpcSecurityGroups_For_ChangeSet
vpcSubnetIds_For_ChangeSet     		=$vpcSubnetIds_For_ChangeSet
sns_Stack_Name 	=$sns_Stack_Name
topic_Name1 	=$topic_Name1
topic_Name2 	=$topic_Name2
endpoint_Email 	=$endpoint_Email
data_Bucket    		=$data_Bucket
scripts_Path   		=$scripts_Path
name_Suffix 		=$name_Suffix
trigger_Bucket  	=$trigger_Bucket
glue_Stack_Name 	=$glue_Stack_Name
layer_Name 			=$layer_Name
lambdaLayer_Stack_Name 		=$lambdaLayer_Stack_Name
lambda_Stack_Name 			=$lambda_Stack_Name
"@

#exit;

$currenttimestamp = Get-Date -Format "MM.dd.yyyy HH:mm:ss K"
$description_For_Version = "adds latest code on ${currenttimestamp} "

##### Deployment steps begin #####
# 1. upload artifacts
aws --profile $account_Name s3 cp ../artifacts/  "s3://$scripts_Path/" --recursive
##copies both wheel file(consumed by glue jobs) and zip file(consumed by lambdas) to $bucket from repo artifacts folder

# 2. Upload individual glue scripts
aws --profile $account_Name s3 cp ../glue/ "s3://$scripts_Path/glue/" --recursive --exclude README.md
##copies glue scripts from your local (or) repo

# 3. Run CloudFormation Glue script
aws --profile $account_Name cloudformation deploy `
    --template-file glue.yml `
    --stack-name $glue_Stack_Name `
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket glueRoleArn=$glue_Role_Arn dbConnection=$db_Connection tagEnvironment=$tag_Environment nameSuffix=$name_Suffix fileNameEnvironment=$file_Name_Environment fileType=$file_Type secretManager=$secret_Manager
##creates(if new), overrides(if existing and changed) & leaves as is(if existing and unchanged) the glue jobs that are defined in the .yml file

# 4. Run CloudFormation Lambda scripts

# a) create common layer
aws --profile $account_Name cloudformation deploy `
    --template-file lambda_common_layer_create_version.yml `
    --stack-name $lambdaLayer_Stack_Name `
    --parameter-overrides dataBucket=$data_Bucket layerName=$layer_Name descriptionForVersion=$description_For_Version

# b) get max layer version to use
$layers_Json = aws --profile $account_Name lambda list-layer-versions `
    --layer-name $layer_Name --max-items 1 | convertfrom-json
$layer_Version = $layers_Json.LayerVersions[0].Version
$layer_With_Version_Arn = "arn:aws:lambda:us-east-1:${account_Profile}:layer:${layer_Name}:${layer_Version}"


###### step 1 begins ######	
# c) deploy lambda function definition sequentially as explained in below 3 steps-
#step 1: creates lambda functions and allows s3 buckets to trigger them by setting invoke permission resource
aws --profile $account_Name cloudformation deploy `
    --template-file lambda_s3_trigger_step1.yml `
    --stack-name $lambda_Stack_Name `
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket triggerBucket=$trigger_Bucket layerWithVersionArn=$layer_With_Version_Arn lambdaRoleArn=$lambda_Role_Arn lambdaRoleArnMoveToCfx=$lambda_Role_Arn_MoveToCfx tagEnvironment=$tag_Environment nameSuffix=$name_Suffix secretManager=$secret_Manager fileNameEnvironment=$file_Name_Environment snsEnvironment=$sns_Environment cfxEnvironment=$cfx_Environment acchistEnvironment=$acchist_Environment cfxBciBucket=$cfx_Bci_Bucket cfxCvsBucket=$cfx_Cvs_Bucket skipDelivery=$skip_Delivery fileType=$file_Type securityGroups=$security_Groups subnetIds=$subnet_Ids
###### step 1 ends ######	

	
###### step 2 begins ######
<#
#step 2.1: execute this step only if you wish to deploy a new s3 bucket for first time in the environment through cft;
#		   however we have s3 bucket created already in the environment on which triggers will be added, so no need to execute this command and that is why it is commented;
#		   if you need a test bucket for testing triggers, uncomment this step and a new bucket will be created out of this step automatically;
#		   this is a one time activity(need not run this step everytime you rerun the stack) and if you execute it by mistake,just an error "Buket already exists" is thrown and #			will not create any issues;
aws --profile $account_Name cloudformation deploy `
    --template-file lambda_s3_trigger_step2.yml `
    --stack-name $lambda_Stack_Name `
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket triggerBucket=$trigger_Bucket layerWithVersionArn=$layer_With_Version_Arn lambdaRoleArn=$lambda_Role_Arn lambdaRoleArnMoveToCfx=$lambda_Role_Arn_MoveToCfx tagEnvironment=$tag_Environment nameSuffix=$name_Suffix secretManager=$secret_Manager fileNameEnvironment=$file_Name_Environment snsEnvironment=$sns_Environment cfxEnvironment=$cfx_Environment acchistEnvironment=$acchist_Environment cfxBciBucket=$cfx_Bci_Bucket cfxCvsBucket=$cfx_Cvs_Bucket skipDelivery=$skip_Delivery fileType=$file_Type securityGroups=$security_Groups subnetIds=$subnet_Ids
#>
	
#step 2.2: execute this step to create the change set and import the existing ${trigger_Bucket} resource to the stack;
aws --profile $account_Name cloudformation create-change-set `
    --template-body file://lambda_s3_trigger_step2.yml `
    --stack-name $lambda_Stack_Name `
    --change-set-name ImportChangeSet `
    --change-set-type IMPORT `
    --resources-to-import "ResourceType=AWS::S3::Bucket,LogicalResourceId=Bucket,ResourceIdentifier={BucketName=${trigger_Bucket}}" `
    --parameters ParameterKey=accountProfile,ParameterValue=$account_Profile ParameterKey=scriptsPath,ParameterValue=$scripts_Path ParameterKey=dataBucket,ParameterValue=$data_Bucket ParameterKey=triggerBucket,ParameterValue=$trigger_Bucket ParameterKey=layerWithVersionArn,ParameterValue=$layer_With_Version_Arn ParameterKey=lambdaRoleArn,ParameterValue=$lambda_Role_Arn ParameterKey=lambdaRoleArnMoveToCfx,ParameterValue=$lambda_Role_Arn_MoveToCfx ParameterKey=tagEnvironment,ParameterValue=$tag_Environment  ParameterKey=nameSuffix,ParameterValue=$name_Suffix ParameterKey=fileNameEnvironment,ParameterValue=$file_Name_Environment ParameterKey=snsEnvironment,ParameterValue=$sns_Environment ParameterKey=cfxEnvironment,ParameterValue=$cfx_Environment ParameterKey=acchistEnvironment,ParameterValue=$acchist_Environment ParameterKey=cfxBciBucket,ParameterValue=$cfx_Bci_Bucket ParameterKey=cfxCvsBucket,ParameterValue=$cfx_Cvs_Bucket ParameterKey=skipDelivery,ParameterValue=$skip_Delivery ParameterKey=fileType,ParameterValue=$file_Type ParameterKey=secretManager,ParameterValue=$secret_Manager ParameterKey=securityGroups,ParameterValue=$vpcSecurityGroups_For_ChangeSet ParameterKey=subnetIds,ParameterValue=$vpcSubnetIds_For_ChangeSet
	
Start-Sleep -s 45

#step 2.3: execute the above created change set
aws --profile $account_Name cloudformation execute-change-set --stack-name $lambda_Stack_Name --change-set-name ImportChangeSet
###### step 2 ends ######

Start-Sleep -s 45

###### step 3 begins ######
#step 3: finally, execute this step to add s3 triggers for lambda functions
aws --profile $account_Name cloudformation deploy `
    --template-file lambda_s3_trigger_step3.yml `
    --stack-name $lambda_Stack_Name `
    --parameter-overrides accountProfile=$account_Profile scriptsPath=$scripts_Path dataBucket=$data_Bucket triggerBucket=$trigger_Bucket layerWithVersionArn=$layer_With_Version_Arn lambdaRoleArn=$lambda_Role_Arn lambdaRoleArnMoveToCfx=$lambda_Role_Arn_MoveToCfx tagEnvironment=$tag_Environment nameSuffix=$name_Suffix secretManager=$secret_Manager fileNameEnvironment=$file_Name_Environment snsEnvironment=$sns_Environment cfxEnvironment=$cfx_Environment acchistEnvironment=$acchist_Environment cfxBciBucket=$cfx_Bci_Bucket cfxCvsBucket=$cfx_Cvs_Bucket skipDelivery=$skip_Delivery fileType=$file_Type securityGroups=$security_Groups subnetIds=$subnet_Ids
###### step 3 ends ######


# 5. Run CloudFormation SNS script
##deploys below 2 SNS topics and adds a subscription in the environment.
##you can add the new subscriptions to the created 2 topics by modifying ${endpoint_Email} and rerunning the below cli command
aws --profile $account_Name cloudformation deploy `
    --template-file sns.yml `
    --stack-name $sns_Stack_Name `
    --parameter-overrides topicName1=$topic_Name1 topicName2=$topic_Name2 endpointEmail=$endpoint_Email
	
	
######################### Deployment Ends ##########################

##### PS variable section ######
##Below are the powershell variables just for refernce that may be helpful to deploy individual developing code and unit test. Nothing to do with current script and that is why it is commented.
	
<#
$account_Name = "ahub_devx"
$account_Profile = "795038802291"
$db_Connection = "irx-accums-phi-rds-clr"
$secret_Manager = "irx-ahub-dv-redshift-cluster"
$tag_Environment = "DEV"		
$file_Name_Environment = "TST"
$cfx_Environment = "sit"
$sns_Environment = "SIT"
$file_Type = "T"
$cfx_Bucket = "irx-nonprod-bci-east-1-sftp-app-outbound"
$security_Groups = "sg-0f181993547c48186 sg-0411ae853409470fd sg-077f4be9869df056a"
$subnet_Ids = "subnet-021888688bf4705c0 subnet-01460daa36a0b1381"
$glue_Role_Arn = "arn:aws:iam::795038802291:role/irx-accum-glue-role"
$lambda_Role_Arn = "arn:aws:iam::795038802291:role/irx-accum-lambda-s3-execute-role"
$lambda_Role_Arn_MoveToCfx = "arn:aws:iam::795038802291:role/irx-accum-lambda-s3-role"
$vpcSecurityGroups_For_ChangeSet = "sg-0f181993547c48186\\sg-0411ae853409470fd\\sg-077f4be9869df056a"
$vpcSubnetIds_For_ChangeSet = "subnet-021888688bf4705c0\\subnet-01460daa36a0b1381"
$sns_Stack_Name = "irx-accumhub-sns"
$topic_Name1 = "AHUB_EMAIL_NOTIF"
$topic_Name2 = "Ahub_structural_error_notify"
$endpoint_Email = "test@email.com"
###above variables would remain constant in SIT and modifications in same environment are not allowed
###below variables can be changed as per the indiviudals choice while unit testing the developing code
$data_Bucket = "irx-accumhub-dev-data"
$scripts_Path = "irx-accumhub-dev-data/deployment"
$name_Suffix = "_mvp2"
$trigger_Bucket = "irx-accumhub-dev-data"
$glue_Stack_Name = "irx-accumhub-glue"
$layer_Name = "irxah-common-py"
$lambdaLayer_Stack_Name = "irx-accumhub-lambda-commonlayer"
$lambda_Stack_Name = "irx-accumhub-lambda-s3-trigger"
#>