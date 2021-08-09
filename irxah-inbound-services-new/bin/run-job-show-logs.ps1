[cmdletbinding(SupportsShouldProcess = $True)]
param(
    [Parameter(Position=1, Mandatory=$true)]
    [String]
    $jobName,
    [String]
    $arguments = ""
)  

if ($arguments -ne "") {
  aws --profile ahub_devx glue start-job-run --job-name $jobName --arguments="$arguments"
} else {
  aws --profile ahub_devx glue start-job-run --job-name $jobName
}

# give it a second to register
Start-Sleep -Seconds 1

# we don't seem to have CF logs while the job is running,
# so we wait till it succeeds or fails
$jobState = "RUNNING"
while ($jobState -like "RUNNING") {
  $jobState=aws --profile ahub_devx glue get-job-runs --job-name $jobName --max-items 1 | jq -r ".JobRuns[0].JobRunState"
  if ($jobState -like "RUNNING") {
    write-host "JobState still $jobState. Will sleep 2 seconds."
    Start-Sleep -Seconds 2
  }
  else {
    break;
  }
}
write-host "JobState = $jobState."

aws --profile ahub_devx logs get-log-events `
  --log-group-name  /aws-glue/python-jobs/error `
  --log-stream-name `
  $(aws --profile ahub_devx glue get-job-runs --job-name $jobName --max-items 1 | jq ".JobRuns[0].Id") `
  | jq -r ".events[].message"
