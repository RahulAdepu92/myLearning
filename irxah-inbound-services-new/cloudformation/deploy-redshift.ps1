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
availability_Zone    =$availability_Zone
redshift_Role_Arn    =$redshift_Role_Arn
kms_Key_Id_Redhsift  =$kms_Key_Id_Redhsift
tag_Environment		 =$tag_Environment
security_Groups		 =$security_Groups
redshift_Glue_Arn    =$redshift_Glue_Arn
glue_Role_Arn  		 =$glue_Role_Arn
tag_Region           =$tag_Region
subnet_Group         =$subnet_Group
"@

#exit;
echo "##### Declaration of variables end #####"

echo "##### CFT deployment steps begin #####"

echo "##### Redhsift cluster deployment begin #####"

# Run CloudFormation redshift cluster script
aws --profile $account_Name cloudformation deploy `
    --template-file redshift_cluster.yml `
    --stack-name irx-accumhub-redshift-cluster `
    --parameter-overrides availabilityZone=$availability_Zone redshift_RoleArn=$redshift_Role_Arn kmsKeyIdRedhsift=$kms_Key_Id_Redhsift `
    tagEnvironment=$tag_Environment securityGroups=$security_Groups redshiftGlueArn=$redshift_Glue_Arn glueRoleArn=$glue_Role_Arn tagRegion=$tag_Region subnetGroup=$subnet_Group