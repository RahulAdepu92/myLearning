#!/bin/bash
# script_root=dirname "$(readlink -f "$0")"
# script_root=python -c "import os; import sys; print(os.path.realpath(sys.argv[1]))"
verbose=0
target=${target:-build}
OPTIND=1 # Resets OPTIND in case we used in in a function

# parse options
while getopts hvt: opt; do
   case $opt in
      h)
         usage
         # [[ "${BASH_SOURCE[0]}" == "${0}" ]] && exit 0
         return 0
         ;;
      v)
         verbose=$((verbose+1))
         ;;
      t)
         target=$OPTARG
         ;;
      *)
         usage >&2
         return 1
         ;;
   esac
done
shift "$((OPTIND-1))" # Discard the options and the setinel --
# Evertyhing that's left in "$@" is a non-option

script_root=$(pwd)
ARTIFACTS="$script_root/artifacts"
COMMON="$script_root/common"
PYTHON3=$(which python3)
PYTHON_CMD=${PYTHON3:-python}
(( verbose > 0 )) && echo "python is $PYTHON_CMD"

function package_wheel {
   # Creates a wheel (.whl) package as required by AWS Glue
    # setup.py likes to be called from within the same folder
    pushd .
    cd $COMMON
    # use python to build the wheel distribution
    # echo "python setup.py bdist_wheel --dist-dir $ARTIFACTS clean --all"
    $PYTHON_CMD setup.py bdist_wheel --dist-dir $ARTIFACTS clean --all

    # bdist leaves some folders around even after `clean`
    rm -rf "$COMMON/irxah_common.egg-info"
    popd
}

function package_lambda {
    # Creates a zip package with the specific folder structure required by AWS Lambda
    python_artifacts="$ARTIFACTS/python"
    version=$($PYTHON_CMD $COMMON/setup.py -V)
    package_name="irxah_common-$version.zip"
    package_path="$python_artifacts/$package_name"

    # 1. Ensure we start with a clean python folder
    rm -rf $python_artifacts

    # 2. copy the files to the artifacts/pyton folder and zip them
    mkdir $python_artifacts
    rsync -avr $COMMON/* $python_artifacts --exclude="*/setup.py"
    # cannot specify a different path as the root so we have to cd into artifacts
    pushd .
    cd $ARTIFACTS
    zip -r $package_name python  -x "*.DS_Store"
    popd

    # 3. Remove the pyton folders and the files just to leave everything tidy
    rm -rf $python_artifacts
    echo "Created $package_path."
}

function usage {
      cat <<USAGE
USAGE: ${0##*/} [-t TARGET_NAME]
Executes different build scenarios.

  -t TARGET_NAME - optional, name of build target.
      One of: build, (that's it for now).
      Default: build.
USAGE
}

# if [ "$v" -gte "1" ]; then
if (( verbose > 0 )); then
   cat <<VARS
Variables: 
   target=$target
   verbose=$verbose
   script_root=$script_root
   ARTIFACTS=$ARTIFACTS
   COMMON=$COMMON
   PYTHON=$PYTHON_CMD

VARS
fi

case $target in
   build)
      echo "Will build now"
      package_wheel
      package_lambda
      ;;
   *)
      echo "Unknown target $target."
      ;;
esac
