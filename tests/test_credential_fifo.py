import os
import subprocess
import sys
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NODE = shutil.which('node')


def test_credential_readers_reject_fifos_without_blocking(tmp_path):
    fifo = tmp_path / 'fifo'; os.mkfifo(fifo, 0o600)
    commands = [
        [sys.executable, '-c', "from src.interfaces.adapter.credential import read_token; import sys; read_token(sys.argv[1])", str(fifo)],
        [sys.executable, '-c', "import sys;sys.path.insert(0,'plugins/hermes-amr');from _client import AmrUdsClient;AmrUdsClient(token_file=sys.argv[1]).ping()", str(fifo)],
    ]
    if NODE:
        commands.append([NODE, '--input-type=module', '-e', "import {AmrUdsClient} from './plugins/openclaw-amr/lib/uds_client.js';new AmrUdsClient({tokenFile:process.argv[1]}).getToken()", str(fifo)])
    for command in commands:
        p = subprocess.run(command, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
        assert p.returncode != 0
