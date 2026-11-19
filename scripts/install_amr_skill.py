#!/usr/bin/env python3
"""Link the repository's canonical AMR skill into local agent skill directories."""
import argparse
from pathlib import Path
import sys

SOURCE = Path(__file__).resolve().parent.parent / 'skills' / 'amr-memory'
ROOTS = (
    '.codex/skills', '.claude/skills', '.gemini/skills', '.pi/agent/skills',
    '.config/opencode/skills', '.hermes/skills', '.openclaw/skills',
    '.dsh/skills', '.zcode/skills',
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--home', type=Path, default=Path.home(), help='Agent home directory')
    parser.add_argument('--dry-run', action='store_true', help='Report links without changing files')
    args = parser.parse_args()
    source = SOURCE.resolve(strict=True)
    if not (source / 'SKILL.md').is_file():
        parser.error('Canonical SKILL.md is missing')
    targets = [args.home.expanduser() / root / 'amr-memory' for root in ROOTS]
    conflicts = [target for target in targets if (target.exists() or target.is_symlink()) and not (target.is_symlink() and target.resolve() == source)]
    if conflicts:
        for target in conflicts:
            print(f'Conflict; existing path was not changed: {target}', file=sys.stderr)
        return 1
    for target in targets:
        if target.is_symlink():
            print(f'Current: {target}')
        elif args.dry_run:
            print(f'Would link: {target} -> {source}')
        else:
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            target.symlink_to(source, target_is_directory=True)
            print(f'Linked: {target} -> {source}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
