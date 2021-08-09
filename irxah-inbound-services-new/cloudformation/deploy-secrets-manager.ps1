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
secret_Manager       =$secret_Manager
secret_Description   =$secret_Description
tag_Environment		 =$tag_Environment
tag_Region           =$tag_Region
redshift_Host        =$redshift_Host
Kms_Key_Id_SecretsManager  =$Kms_Key_Id_SecretsManager
"@

#exit;
echo "##### Declaration of variables end #####"

echo "##### CFT deployment steps begin #####"

echo "##### Secrets Manager deployment begin #####"

# Run CloudFormation redshift cluster script
aws --profile $account_Name cloudformation deploy `
    --template-file secrets_manager.yml `
    --stack-name irx-accumhub-secrets-manager `
    --parameter-overrides secretManager=$secret_Manager secretDescription=$secret_Description  `
    tagEnvironment=$tag_Environment tagRegion=$tag_Region redshiftHost=$redshift_Host KmsKeyIdSecretsManager=$Kms_Key_Id_SecretsManager