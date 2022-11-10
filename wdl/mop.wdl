version 1.0

workflow Mop {
    input {
        String workspace_namespace
        String workspace_name
        String user
    }
    call MopTask {
        input:
            workspace_namespace = workspace_namespace,
            workspace_name = workspace_name,
            user = user
    }
}

task MopTask {
    input {
        String workspace_namespace
        String workspace_name
        String user
    }
    
    command <<<
        set -xeuo pipefail
        
        cat <<'EOF' > script.py
import firecloud.api as fapi
import firecloud.fiss as fiss
import argparse
from google.cloud import storage
import sys
from fnmatch import fnmatchcase
from six import string_types
from datetime import datetime
from google.cloud import bigquery
import pytz

class MopArgs():
    def __init__(self, workspace_namepspace, workspace_name, verbose, include, exclude, dry_run, yes):
        self.workspace_namespace = workspace_namepspace
        self.workspace_name = workspace_name
        self.verbose = verbose
        self.include = include
        self.exclude = exclude
        self.dry_run = dry_run
        self.yes = yes


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def mop(args, user):
    # First retrieve the workspace to get bucket information
    if args.verbose:
        print("Retrieving workspace information...")
    fields = "workspace.bucketName,workspace.name,workspace.attributes"
    r = fapi.get_workspace(args.workspace_namespace, args.workspace_name, fields=fields)
    fapi._check_response_code(r, 200)
    workspace = r.json()
    bucket = workspace['workspace']['bucketName']
    bucket_prefix = 'gs://' + bucket
    workspace_name = workspace['workspace']['name']

    if args.verbose:
        print("{} -- {}".format(workspace_name, bucket_prefix))
    
    # Handle Basic Values, Compound data structures, and Nestings thereof
    def update_referenced_files(referenced_files, attrs, bucket_prefix):
        for attr in attrs:
            # 1-D array attributes are dicts with the values stored in 'items'
            if isinstance(attr, dict) and attr.get('itemsType') == 'AttributeValue':
                update_referenced_files(referenced_files, attr['items'], bucket_prefix)
            # Compound data structures resolve to dicts
            elif isinstance(attr, dict):
                update_referenced_files(referenced_files, attr.values(), bucket_prefix)
            # Nested arrays resolve to lists
            elif isinstance(attr, list):
                update_referenced_files(referenced_files, attr, bucket_prefix)
            elif isinstance(attr, string_types) and attr.startswith(bucket_prefix):
                referenced_files.add(attr)

    referenced_files = set()
    update_referenced_files(referenced_files,
                            workspace['workspace']['attributes'].values(),
                            bucket_prefix)

    ## Now list files present in the bucket
    def list_blob_gen(bucket_name):
        """Generate the list of blobs in the bucket and size of each blob
        Args:
            bucket_name (str): Bucket Name
        Yields:
            tuple: File name and the size of the file
        """
        client_st = storage.Client(project=args.workspace_namespace)
        blobs = client_st.list_blobs(bucket_name)
        for blob in blobs:
            yield ("gs://{}/{}".format(blob.bucket.name, blob.name), int(blob.size))


    try:
        # store size of each file in bucket to report recovered space
        bucket_file_sizes = {b[0]: b[1] for b in list_blob_gen(bucket)}
        
        # Now make a call to the API for the user's submission information.
        user_submission_request = fapi.list_submissions(args.workspace_namespace, args.workspace_name)

        # Check if API call was successful, in the case of failure, the function will return an error
        fapi._check_response_code(user_submission_request, 200)

        # Sort user submission ids for future bucket file verification
        submission_ids = set(item['submissionId'] for item in user_submission_request.json())

        # Check to see if bucket file path contain the user's submission id
        # to ensure deletion of files in the submission directories only.
        # Splits the bucket file: gs://<bucket>/<submission_id>/<file_path> or
        #                         gs://<bucket>/submissions/<submission_id>/<file_path>, by the '/' symbol
        # and stores values in a 6 length array: ['gs:', '' , <bucket>, <submission_id>, <workflow_name>, <file_path>] or
        #                                        ['gs:', '' , <bucket>, 'submissions', <submission_id>, <file_path>]
        # to extract the submission id from the 4th or 5th element (index 3 or 4) of the array
        bucket_files = set()
        for bucket_file in bucket_file_sizes:
            for sub_id in bucket_file.split('/', 5)[3:5]:
                if sub_id in submission_ids:
                    bucket_files.add(bucket_file)
                    break
        
    except Exception as e:
        eprint("Error retrieving files from bucket:" +
               "\n\t{}: {}".format(type(e).__name__, e))
        return 1

    if args.verbose:
        num = len(bucket_files)
        if args.verbose:
            print("Found {} files in bucket {}".format(num, bucket))

    # Now build a set of files that are referenced in the bucket
    # 1. Get a list of the entity types in the workspace
    r = fapi.list_entity_types(args.workspace_namespace, args.workspace_name)
    fapi._check_response_code(r, 200)
    entity_types = r.json().keys()

    # 2. For each entity type, request all the entities
    for etype in entity_types:
        if args.verbose:
            print("Getting annotations for " + etype + " entities...")
        # use the paginated version of the query
        entities = fiss._entity_paginator(args.workspace_namespace, args.workspace_name, etype,
                              page_size=1000, filter_terms=None,
                              sort_direction="asc")

        for entity in entities:
            update_referenced_files(referenced_files,
                                    entity['attributes'].values(),
                                    bucket_prefix)

    if args.verbose:
        num = len(referenced_files)
        print("Found {} referenced files in workspace {}".format(num, workspace_name))

    # Set difference shows files in bucket that aren't referenced
    unreferenced_files = bucket_files - referenced_files

    # Filter out files like .logs and rc.txt
    def can_delete(f):
        '''Return true if this file should not be deleted in a mop.'''
        filename = f.rsplit('/', 1)[-1]
        # Don't delete logs
        if filename.endswith('.log'):
            return False
        # Don't delete return codes from jobs
        if filename.endswith('-rc.txt'):
            return False
        if filename == "rc":
            return False
        if filename == "memory_retry_rc":
            return False
        # Don't delete tool's exec.sh or script
        if filename in ('exec.sh', 'script'):
            return False
        # keep stdout, stderr, and output
        if filename in ('stderr', 'stdout', 'output'):
            return False
        # Don't delete utility scripts
        if filename in ('gcs_localization.sh', 'gcs_delocalization.sh', 'gcs_transfer.sh'):
            return False
        # Only delete specified unreferenced files
        if args.include:
            for glob in args.include:
                if fnmatchcase(filename, glob):
                    return True
            return False
        # Don't delete specified unreferenced files
        if args.exclude:
            for glob in args.exclude:
                if fnmatchcase(filename, glob):
                    return False

        return True

    deletable_files = [f for f in unreferenced_files if can_delete(f)]

    if len(deletable_files) == 0:
        if args.verbose:
            print("No files to mop in " + workspace['workspace']['name'])
        return 0
    
    units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    def human_readable_size(size_in_bytes):
        '''Takes a bytes value and returns a human-readable string with an
        appropriate unit conversion'''
        reduce_count = 0
        while size_in_bytes >= 1024.0 and reduce_count < 5:
            size_in_bytes /= 1024.0
            reduce_count += 1
        size_str = "{:.2f}".format(size_in_bytes) if reduce_count > 0 else str(size_in_bytes)
        return "{} {}".format(size_str, units[reduce_count])
    
    deletable_size = human_readable_size(sum(bucket_file_sizes[f]
                                             for f in deletable_files))

    if args.verbose or args.dry_run:
        print("Found {} files to delete:\n".format(len(deletable_files)) +
              "\n".join("{}  {}".format(human_readable_size(bucket_file_sizes[f]).rjust(11), f)
                        for f in deletable_files) +
              '\nTotal Size: {}\n'.format(deletable_size))
    
    mop_event = {
            'user': user,
            'datetime': datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d-%H-%M-%S'),
            'workspace_namespace': args.workspace_namespace,
            'workspace_name': args.workspace_name,
            'size_deleted': sum(bucket_file_sizes[f] for f in deletable_files)
        }
    db = bigquery.Client(project='broad-dsde-methods-automop')
    db.insert_rows(db.get_table('broad-dsde-methods-automop.automop_test.mop_events'), [mop_event])


    if args.dry_run or not args.yes:
        return 0

    # Pipe the deletable_files into gsutil rm to remove them
    #gsrm_args = ['gsutil', '-m', 'rm', '-I']
    #PIPE = subprocess.PIPE
    #STDOUT=subprocess.STDOUT
    #if args.verbose:
    #    print("Deleting files with gsutil...")
    #gsrm_proc = subprocess.Popen(gsrm_args, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
    ## Pipe the deletable_files into gsutil
    #result = gsrm_proc.communicate(input='\n'.join(deletable_files).encode())[0]
    #if args.verbose:
    #    if type(result) == bytes:
    #        result = result.decode()
    #    print(result.rstrip())
    return 0


def main(workspace_namespace, workspace_name, user):
    mop_args = MopArgs(workspace_namespace, workspace_name, True, None, None, True, False)
    mop(mop_args, user)


if __name__ == '__main__':
    main('~{workspace_namespace}', '~{workspace_name}', '~{user}')
EOF
        python script.py
    >>>
    
    runtime {
        docker: "us.gcr.io/broad-dsde-methods/automop:0.1"
        preemptible: 0
        memory: "16 GB"
        disks: "local-disk 20 HDD"
    }
    output {
        File stdout = stdout()
        File stderr = stderr()
    }
}