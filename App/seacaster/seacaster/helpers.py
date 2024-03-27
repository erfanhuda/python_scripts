import logging

class _OutputLogger(logging.Logger):
    def __init__(self, _name):
        super().__init__(self)
        self.name = _name