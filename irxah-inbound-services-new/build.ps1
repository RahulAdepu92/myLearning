[cmdletbinding(SupportsShouldProcess = $True)]
param(
    [Parameter(Position = 1)]
    [String]
    $target = "build",
    [String]
    $file
)


$ARTIFACTS = "$PSScriptRoot/artifacts"
$COMMON = "$PSScriptRoot/common"
$SUPPLEMENTAL_LIBS = "$PSScriptRoot/supplemental-libraries"

function package_wheel {
    <#
        .SYNOPSIS
        Creates a wheel (.whl) package as required by AWS Glue
    #>
    # setup.py likes to be called from within the same folder
    Push-Location
    Set-Location $COMMON
    # use python to build the wheel distribution
    python setup.py bdist_wheel --dist-dir $ARTIFACTS clean --all

    # bdist leaves some folders around even after `clean`
    Remove-Item -Force -Recurse "$COMMON/irxah_common.egg-info"
    Pop-Location
}

function package_lambda {
    <#
        .SYNOPSIS
        Creates a zip package with the specific folder structure required by AWS Lambda
    #>
    $python_artifacts = "$ARTIFACTS/python"
    $version = (python $COMMON/setup.py -V) | Out-String
    $version = $version.Trim()
    $package_name = "irxah_common-$version.zip"
    $package_path = "$python_artifacts/$package_name"

    # 1. Ensure we start with a clean python folder
    Remove-Item -Force -Recurse $python_artifacts -ErrorAction SilentlyContinue

    # 2. copy the files to the artifacts/pyton folder and zip them
    # New-Item likes to list the path. let's silent it with | Out-Null
    New-Item -ItemType Directory -Force $python_artifacts | Out-Null
    Copy-Item -Path "$COMMON/*" -Recurse -Force -Destination $python_artifacts -Exclude "setup.py"

    # install requirements into artifacts/python
    # normally we'd install from a requirements file,
    # pip install --target="$python_artifacts" -r requirements-lambda.txt
    # But since we already have the wheel files for Glue, might as well install 
    # from the downloaded folder.
    Get-ChildItem $SUPPLEMENTAL_LIBS -Filter *.whl | `
    ForEach-Object { pip --disable-pip-version-check install  --upgrade --target="$python_artifacts" $_.FullName }

    if ($IsWindows -or ($ENV:OS -eq "Windows_NT")) {
        icacls $python_artifacts /grant Everyone:F /t
    }
    else {
        chmod -R 755 $python_artifacts
    }

    # an .eggs folder seems to get created. it upsets Lambda, so we're removing it
    Remove-Item -Force -Recurse $python_artifacts/.eggs -ErrorAction SilentlyContinue

    if ($IsWindows -or ($ENV:OS -eq "Windows_NT")) {
        # Lambda Layer upload fails when zipped like above..
        Compress-Archive -Force -Path $python_artifacts -Destination "$ARTIFACTS/$package_name"
    }
    else {
        # TODO  : Please  help remove this ugliness ..

        # Includes the fully qualified path 
        cd $python_artifacts
        #  by doing cd.. (below) you will reach to a directory like /irxah-inbound-services/artifacts
        cd ..
        # And now gets zipped in such a way that when unzipped it begins with python level folder only
        zip -r -X $ARTIFACTS/$package_name python

        # Once done executing now get back from the /irxah-inbound-services/artifacts to the /irxah-inbound-services/
        
        cd .. 


    }
    
    #

    # 3. Remove the pyton folders and the files just to leave everything tidy
    Remove-Item -Force -Recurse $python_artifacts
    Write-Output "Created $package_name."
}

function split_sql {
    param ([String] $sqlFile)

    if ([string]::IsNullOrWhiteSpace($sqlFile)) {
        throw "Missing parameter -file pointing to a sql file."
    }
    if ($sqlFile -notmatch ".sql$") {
        throw "splitSql available only for .sql files."
    }

    $fullPath = Resolve-Path $sqlFile
    $folder = Split-Path -Resolve -Path $sqlFile
    $fileName = Split-Path -Leaf $sqlFile
    $baseFileName = [io.path]::GetFileNameWithoutExtension($fileName)
    Write-Host "Will split $fileName into $folder :"

    $lines = [io.File]::ReadAllLines($fullPath)
    
    function makeFile {
        param ([Int] $counter)
        $fileFormat = "${baseFileName}-{0:00}.sql" -f $counter
        #Write-Host "Would create $fileFormat in $folder"
        $filePath = Join-Path -Path $folder -ChildPath $fileFormat
        New-Item -Force -Path $folder -Name $fileFormat -ItemType "file" -Value "-- $baseFileName  part $counter`n"
        return $filePath
    }

    $fileCounter = 1
    $currentFile = makeFile($fileCounter)
    #Write-Host "$fileCounter. $currentFile"
    foreach($line in $lines) {
        # $line | Out-File -FilePath $currentFile
        Add-Content -Path $currentFile -Value $line
        # Write-Host -NoNewLine "."
        if ($line -match "\;\s*$") {
            # last line to go in the previous file
            $fileCounter += 1;
            $currentFile = makeFile($fileCounter)
            #Write-Host "$fileCounter. $currentFile"
        }
    }

    Write-Host "Wrote ${baseFileName}-01.sql through $currentFile into $folder"
}

if ($target -like "build") { package_wheel; package_lambda; }
elseif ($target -like "buildZip") { package_lambda; }
elseif ($target -like "buildWheel") { package_wheel; }
elseif ($target -like "splitSql") { split_sql($file); }
else { Write-Output "Unknown target: $target. Known targets: build, buildZip, buildWheel, splitSql." }
