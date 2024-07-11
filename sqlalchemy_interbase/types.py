import datetime as dt

from typing import Any
from typing import Optional
from sqlalchemy import Dialect, types as sqltypes


# Character set of BINARY/VARBINARY
BINARY_CHARSET = "OCTETS"

# Character set of NCHAR/NVARCHAR
NATIONAL_CHARSET = "ISO8859_1"


class _IBString(sqltypes.String):
    render_bind_cast = True

    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length, collation)
        self.charset = charset


class IBCHAR(_IBString):
    __visit_name__ = "CHAR"

    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length, charset, collation)


class IBBINARY(IBCHAR):
    __visit_name__ = "BINARY"

    # Synonym for CHAR(n) CHARACTER SET OCTETS
    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length, BINARY_CHARSET)


class IBNCHAR(IBCHAR):
    __visit_name__ = "NCHAR"

    # Synonym for CHAR(n) CHARACTER SET ISO8859_1
    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length, NATIONAL_CHARSET)


class IBVARCHAR(_IBString):
    __visit_name__ = "VARCHAR"

    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length, charset, collation)


class IBVARBINARY(IBVARCHAR):
    __visit_name__ = "VARBINARY"

    # Synonym for VARCHAR(n) CHARACTER SET OCTETS
    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length, BINARY_CHARSET)


class IBNVARCHAR(IBVARCHAR):
    __visit_name__ = "NVARCHAR"

    # Synonym for VARCHAR(n) CHARACTER SET ISO8859_1
    def __init__(self, length=None, charset=None, collation=None):
        super().__init__(length, NATIONAL_CHARSET)


class _IBNumeric(sqltypes.Numeric):
    render_bind_cast = True

    def bind_processor(self, dialect):
        return None  # Dialect supports_native_decimal = True (no processor needed)


class IBFLOAT(_IBNumeric, sqltypes.FLOAT):
    __visit_name__ = "FLOAT"


class IBDOUBLE_PRECISION(_IBNumeric, sqltypes.DOUBLE_PRECISION):
    __visit_name__ = "DOUBLE_PRECISION"


class IBDECFLOAT(_IBNumeric):
    __visit_name__ = "DECFLOAT"


class IBREAL(IBFLOAT):
    __visit_name__ = "REAL"

    # Synonym for FLOAT
    def __init__(self, precision=None, scale=None):
        super().__init__(None, None)


class _IBFixedPoint(_IBNumeric):
    def __init__(
        self,
        precision=None,
        scale=None,
        decimal_return_scale=None,
        asdecimal=None,
    ):
        super().__init__(
            precision, scale, decimal_return_scale, asdecimal=True
        )


class IBDECIMAL(_IBFixedPoint):
    __visit_name__ = "DECIMAL"


class IBNUMERIC(_IBFixedPoint):
    __visit_name__ = "NUMERIC"


class IBDATE(sqltypes.DATE):
    render_bind_cast = True


class IBTIME(sqltypes.TIME):
    render_bind_cast = True


class IBTIMESTAMP(sqltypes.TIMESTAMP):
    render_bind_cast = True


class _IBInteger(sqltypes.Integer):
    render_bind_cast = True


class IBSMALLINT(_IBInteger):
    __visit_name__ = "SMALLINT"


class IBINTEGER(_IBInteger):
    __visit_name__ = "INTEGER"


class IBBIGINT(_IBInteger):
    __visit_name__ = "BIGINT"


class IBINT128(_IBInteger):
    __visit_name__ = "INT128"


class IBBOOLEAN(sqltypes.BOOLEAN):
    render_bind_cast = True


class _IBLargeBinary(sqltypes.LargeBinary):
    render_bind_cast = True

    def __init__(
        self, subtype=None, segment_size=None, charset=None, collation=None
    ):
        super().__init__()
        self.subtype = subtype
        self.segment_size = segment_size
        self.charset = charset
        self.collation = collation

    def bind_processor(self, dialect):
        def process(value):
            return None if value is None else bytes(value)

        return process


class IBBLOB(_IBLargeBinary, sqltypes.BLOB):
    __visit_name__ = "BLOB"

    def __init__(
        self,
        segment_size=None,
    ):
        super().__init__(0, segment_size)


class IBTEXT(_IBLargeBinary, sqltypes.TEXT):
    __visit_name__ = "BLOB"

    def __init__(
        self,
        segment_size=None,
        charset=None,
        collation=None,
    ):
        super().__init__(1, segment_size, charset, collation)


class _IBNumericInterval(_IBNumeric):
    # NUMERIC(18,9) -- Used for _IBInterval storage
    def __init__(self):
        super().__init__(precision=18, scale=9)


class _IBInterval(sqltypes.Interval):
    """A type for ``datetime.timedelta()`` objects.

    Value is stored as number of days.
    """

    # ToDo: Fix operations with TIME datatype (operand must be in seconds, not in days)
    #   https://firebirdsql.org/file/documentation/html/en/refdocs/fblangref50/firebird-50-language-reference.html#fblangref50-datatypes-datetimeops

    impl = _IBNumericInterval
    cache_ok = True

    def __init__(self):
        super().__init__(native=False)

    def bind_processor(self, dialect: Dialect):
        impl_processor = self.impl_instance.bind_processor(dialect)
        if impl_processor:
            fixed_impl_processor = impl_processor

            def process(value: Optional[dt.timedelta]):
                dt_value = (
                    value.total_seconds() / 86400
                    if value is not None
                    else None
                )
                return fixed_impl_processor(dt_value)

        else:

            def process(value: Optional[dt.timedelta]):
                return (
                    value.total_seconds() / 86400
                    if value is not None
                    else None
                )

        return process

    def result_processor(self, dialect: Dialect, coltype: Any):
        impl_processor = self.impl_instance.result_processor(dialect, coltype)
        if impl_processor:
            fixed_impl_processor = impl_processor

            def process(value: Any) -> Optional[dt.timedelta]:
                dt_value = fixed_impl_processor(value)
                if dt_value is None:
                    return None
                return dt.timedelta(days=dt_value)

        else:

            def process(value: Any) -> Optional[dt.timedelta]:
                return dt.timedelta(days=value) if value is not None else None

        return process