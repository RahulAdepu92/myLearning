# Deployment Instructions

In the following, `$/` designates the top-level of the repository.

1. Create a python wheel package by executing `build.ps1` or `build.sh`
   from `$/`.

```ps
.\build.ps1
```

The `.whl` package will be in `$/artifacts`
and named `irxah_common-0.1-py3-none-any.whl` or similar.

2. Upload the wheel package to an S3 bucket:

    * `aws-glue-scripts-474156701944-us-east-1` for shared dev
    * `aws-glue-scripts-795038802291-us-east-1` for dedicated dev
    * `aws-glue-scripts-974356221399-us-east-1` for pre-prod

3. When creating the initial job, add the *full S3 path to the wheel file*
  into the *Python library path* under the
  *Security configuration, script libraries, and job parameters (optional)*
  section.

4. Inside the job code, import from the module and pass the job
   command line arguments to the function (or set up environment variables):

```python
import sys
from irxah.jobs import process_bci_file

process_bci_file(sys.argv)
```

To update a wheel, simply upload the new version (with the same name)
to the above S3 bucket.  
Glue unpacks the wheel file upon execution, so it will always pick up
the latest code from S3.

If a version needs to be preserve, update the version number in
`$/common/setup.py`, rebuild the wheel and update the new version to S3.
Once uploaded, select the new version in "Python library path" section
of the job detail.