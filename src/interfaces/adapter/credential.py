"""Read a private local bearer credential without exposing it in errors."""
import os
import stat


def read_token(path):
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        with os.fdopen(fd, 'rb') as f:
            info = os.fstat(f.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) not in (0o400, 0o600):
                raise ValueError('invalid file')
            token = f.read(4097).decode('ascii').strip()
        if not 32 <= len(token) <= 4096 or any(not c.isprintable() or c.isspace() for c in token):
            raise ValueError('invalid token')
        return token
    except (OSError, UnicodeError, ValueError, TypeError):
        raise ValueError('credential unavailable or invalid') from None
