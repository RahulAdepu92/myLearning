[cmdletbinding(SupportsShouldProcess = $True)]
param(
    [Parameter(Position=1, Mandatory=$true)]
    [String]
    $jobName,
    [Parameter(Position=2, Mandatory=$true)]
    [String]
    $uploadPath,
    [String]
    $arguments = ""
)  
# rebuild-and-run-glue-job.ps1 -jobName ph_export_test1 -uploadPath "aws-glue-scripts-795038802291-us-east-1/philip/" -arguments "--CLIENT_FILE_ID=22"
./build.ps1 buildwheel
aws --profile ahub_devx  s3 cp ./artifacts/irxah_common-0.1-py3-none-any.whl s3://$uploadPath
if ($arguments -ne "") {
    aws --profile ahub_devx glue start-job-run --job-name $jobName --arguments="$arguments"
} else {
    aws --profile ahub_devx glue start-job-run --job-name $jobName
}