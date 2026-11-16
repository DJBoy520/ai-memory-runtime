"""Offline AMR identity inventory. Exported configuration is not activated."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import sys
from src.agent_access.registry import Registry, RegistryError
from src.agent_access.activation import ActivationError, activate_deny_all, activate_registry_generation, check_unit_paths, registry_status


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--registry', default=os.path.expanduser('~/.config/amr/agents'))
    commands = p.add_subparsers(dest='command', required=True)
    commands.add_parser('list')
    commands.add_parser('status')
    for name in ('add', 'grant'):
        c = commands.add_parser(name); c.add_argument('agent'); c.add_argument('--tools', required=True, help='comma-separated canonical tool names')
        if name == 'add': c.add_argument('--socket')
    for name in ('disable', 'enable', 'rotate'):
        c = commands.add_parser(name); c.add_argument('agent')
    c = commands.add_parser('render'); c.add_argument('--output', required=True); c.add_argument('--adapter-base', required=True); c.add_argument('--remote-base')
    c = commands.add_parser('import'); c.add_argument('--credentials', required=True); c.add_argument('--adapter')
    c = commands.add_parser('activate'); c.add_argument('generation')
    commands.add_parser('activate-deny-all')
    args = p.parse_args(argv)
    try:
        reg = Registry(args.registry)
        if args.command == 'list':
            print(json.dumps(reg.list_agents(), ensure_ascii=False, indent=2))
        elif args.command == 'status':
            print(json.dumps(registry_status(reg), ensure_ascii=False, indent=2))
        elif args.command == 'add': reg.add(args.agent, args.tools.split(','), args.socket)
        elif args.command == 'grant': reg.set_tools(args.agent, args.tools.split(','))
        elif args.command == 'disable': reg.disable(args.agent)
        elif args.command == 'enable': reg.enable(args.agent)
        elif args.command == 'rotate': reg.rotate(args.agent)
        elif args.command == 'import': reg.import_existing(args.credentials, args.adapter)
        elif args.command == 'activate':
            check_unit_paths(reg.root)
            activate_registry_generation(reg, args.generation)
            print('Snapshot activated; both services restarted. Restart affected agents and verify pending spool replay.')
        elif args.command == 'activate-deny-all':
            check_unit_paths(reg.root)
            activate_deny_all(reg)
            print('Both services stopped; current configuration is invalidated.')
        elif args.command == 'render':
            with open(args.adapter_base, encoding='utf-8') as f: base = json.load(f)
            remote = None
            if args.remote_base:
                with open(args.remote_base, encoding='utf-8') as f: remote = json.load(f)
            reg.render(args.output, base, remote)
            print('Snapshot created; services have not been changed or restarted.')
        if args.command not in ('list', 'status', 'render', 'activate', 'activate-deny-all'):
            print('Registry updated; running services are unchanged until activation.')
        return 0
    except (RegistryError, ActivationError, OSError, ValueError) as exc:
        print(f'agent registry error: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
