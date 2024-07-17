# sqlalchemy_interbase/__init__.py
# Copyright (C) 2024 Dmytro Yaroshenko
# This module is released under the MIT License
__version__ = "0.0.1b0"

from sqlalchemy_interbase.base import IBDialect
from sqlalchemy_interbase.types import *

# # The code bellow required if library not installed with dialect endpoint registration
# from sqlalchemy.dialects import registry
# base.dialect = dialect = IBDialect
# registry.register("interbase", __name__, "IBDialect")
