version 1.0

workflow Mop {
    input {
        String workspace_namespace
        String workspace_name
        String user
        Boolean dry_run
    }
    call MopTask {
        input:
            workspace_namespace = workspace_namespace,
            workspace_name = workspace_name,
            user = user,
            dry_run = dry_run
    }
}

task MopTask {
    input {
        String workspace_namespace
        String workspace_name
        String user
        Boolean dry_run
    }
    
    command <<<
        set -xeuo pipefail
        
        cat <<'EOF' > script.py
import subprocess
from datetime import datetime
from google.cloud import bigquery
import pytz

def main(workspace_namespace, workspace_name, user):
    units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']

    mop_process = subprocess.Popen(['fissfc', '--yes', '--verbose', 'mop', '-w', workspace_name, '-p', workspace_namespace~{if dry_run then ", '--dry-run'" else ""}],
        stdout=subprocess.PIPE)
    
    size_found = False
    run_successful = False
    with open('fissfc_log.log', 'w') as fissfc_log:
        for line in iter(mop_process.stdout.readline, b''):
            line = line.decode()
            fissfc_log.write(line)
            if line.startswith('Total Size: '):
                size_with_unit = line.rstrip()[12:]
                size, unit = size_with_unit.split(' ')
                size_in_bytes = int(float(size) * 1024 ** units.index(unit))
                size_found = True
            if line.startswith('No files to mop in'):
                size_in_bytes = 0
                print('No files to mop.')
                size_found = True
                run_successful = True
            if line.startswith('Operation completed over'):
                print('Mopping complete!')
                run_successful = True
    
    if not size_found:
        raise RuntimeError('No total deleted size found in fissfc output.')
    
    if not run_successful:
        raise RuntimeError('Did not receive "Operation completed" message from fissfc output.')
    
    mop_event = {
            'user': user,
            'datetime': datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d-%H-%M-%S'),
            'workspace_namespace': workspace_namespace,
            'workspace_name': workspace_name,
            'size_deleted': size_in_bytes
        }
    db = bigquery.Client(project='broad-dsde-methods-automop')
    db.insert_rows(db.get_table('broad-dsde-methods-automop.automop.mop_events'), [mop_event])


if __name__ == '__main__':
    main('~{workspace_namespace}', '~{workspace_name}', '~{user}')
EOF
        python script.py
    >>>

    output {
        File fissfc_log = "fissfc_log.log"
    }
    
    runtime {
        docker: "us.gcr.io/broad-dsde-methods/automop:0.1"
        preemptible: 0
        memory: "16 GB"
        disks: "local-disk 20 HDD"
    }
}