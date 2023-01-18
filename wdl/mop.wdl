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

    mop_process = subprocess.Popen(['fissfc', '--verbose', 'mop', '-w', workspace_name, '-p', workspace_namespace~{if dry_run then ", '--dry-run'" else ""}],
        stdout=subprocess.PIPE)
    for line in iter(mop_process.stdout.readline, b''):
        line = line.decode()
        if line.startswith('Total Size: '):
            size_with_unit = line.rstrip()[12:]
            size, unit = size_with_unit.split(' ')
            size_in_bytes = int(float(size) * 1024 ** units.index(unit))
    
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
    
    runtime {
        docker: "us.gcr.io/broad-dsde-methods/automop:0.1"
        preemptible: 0
        memory: "16 GB"
        disks: "local-disk 20 HDD"
    }
}