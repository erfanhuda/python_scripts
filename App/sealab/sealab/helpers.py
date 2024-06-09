import logging
import os
from dataclasses import dataclass


class _LoggingHandler(logging.Handler):
    def __init__(self):
        super().__init__(self, level=logging.DEBUG)
        self._format = logging.Formatter(
            fmt=None, datefmt="%Y-%m-%d - %%H:%M:%S", validate=False)


@dataclass
class _ClientID:
    _user: str = os.getlogin()
    _pid: str = os.getpid()
    _cwd: str = os.getcwd()


class _RunningLogger(logging.Logger):
    def __init__(self, _name: str, _format=logging.Formatter) -> None:
        super().__init__(self)
        self.name = _name
        self._format = _format


class _OutputLogger(logging.StreamHandler):
    def __init__(self, _name: str, _format=logging.Formatter) -> None:
        super().__init__(self)
        self.name = _name
        self._format = _format

    def _save(self):
        pass
