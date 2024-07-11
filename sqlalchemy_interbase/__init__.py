# sqlalchemy_interbase/__init__.py
# Copyright (C) 2024 Dmytro Yaroshenko
# This module is released under the MIT License: http://www.opensource.org/licenses/mit-license.php
__version__ = "0.0.1b0"

from sqlalchemy_interbase.base import IBDialect
from sqlalchemy_interbase.types import *
from sqlalchemy.dialects import registry

base.dialect = dialect = IBDialect
registry.register("interbase", __name__, "IBDialect")