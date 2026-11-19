import subprocess
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'install_amr_skill.py'
SOURCE = Path(__file__).resolve().parents[1] / 'skills' / 'amr-memory'
TARGETS = ['.codex/skills', '.claude/skills', '.gemini/skills', '.pi/agent/skills', '.config/opencode/skills', '.hermes/skills', '.openclaw/skills', '.dsh/skills', '.zcode/skills']


def run_install(home, *args):
    return subprocess.run([sys.executable, str(SCRIPT), '--home', str(home), *args], capture_output=True, text=True)


def test_installer_links_authoritative_skill_and_is_idempotent(tmp_path):
    dry = run_install(tmp_path, '--dry-run')
    assert dry.returncode == 0
    assert not (tmp_path / '.codex').exists()
    assert run_install(tmp_path).returncode == 0
    for root in TARGETS:
        link = tmp_path / root / 'amr-memory'
        assert link.is_symlink()
        assert link.resolve() == SOURCE.resolve()
        assert (link / 'SKILL.md').read_text() == (SOURCE / 'SKILL.md').read_text()
    assert run_install(tmp_path).returncode == 0


def test_installer_refuses_conflict_before_modifying_other_agents(tmp_path):
    conflict = tmp_path / '.hermes/skills/amr-memory'
    conflict.mkdir(parents=True)
    (conflict / 'SKILL.md').write_text('existing user skill')
    result = run_install(tmp_path)
    assert result.returncode != 0
    assert (conflict / 'SKILL.md').read_text() == 'existing user skill'
    assert not (tmp_path / '.codex').exists()
