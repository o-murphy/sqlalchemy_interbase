import sqlalchemy_firebird.types as fb_types
from sqlalchemy import exc, text
from sqlalchemy import types as sa_types
from sqlalchemy import util
from sqlalchemy.dialects import registry
from sqlalchemy.engine import reflection
from sqlalchemy_firebird.base import FBDialect, FBDDLCompiler

EXPRESSION_SEPARATOR = "||"


class IBDDLCompiler(FBDDLCompiler):

    def visit_create_sequence(self, create, prefix=None, **kw):
        text = "CREATE GENERATOR "
        if create.if_not_exists:
            text += "IF NOT EXISTS "
        text += self.preparer.format_sequence(create.element)

        if prefix:
            text += prefix
        options = self.get_identity_options(create.element)
        if options:
            text += " " + options
        return text


# Define your custom Firebird dialect
class IBDialect(FBDialect):
    driver = 'interbase'

    ddl_compiler = IBDDLCompiler

    @classmethod
    def dbapi(cls):
        import interbase
        return interbase

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user', password='password', database='dsn')
        opts.update(url.query)
        opts.pop('port', None)

        if 'charset' in opts:
            opts['charset'] = opts['charset'].upper()
        else:
            opts['charset'] = 'WIN1252'

        # Ensure the DSN is correctly formed
        dsn = opts.pop('dsn')
        opts['host'] = None
        opts['dsn'] = f'localhost/3051:{dsn}'

        return [], opts

    def initialize(self, connection):
        self.server_version_info = (2, 5, 0)  # Replace with actual logic to fetch server version
        if self.server_version_info < (3,):
            from sqlalchemy_firebird.fb_info25 import MAX_IDENTIFIER_LENGTH, RESERVED_WORDS
            self.supports_identity_columns = False
            self.supports_native_boolean = False
        elif self.server_version_info < (4,):
            from sqlalchemy_firebird.fb_info30 import MAX_IDENTIFIER_LENGTH, RESERVED_WORDS
        else:
            from sqlalchemy_firebird.fb_info40 import MAX_IDENTIFIER_LENGTH, RESERVED_WORDS

        self.max_identifier_length = MAX_IDENTIFIER_LENGTH
        self.preparer.reserved_words = RESERVED_WORDS

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        has_table_query = """
            SELECT 1 AS has_table
            FROM rdb$relations
            WHERE rdb$relation_name = LTRIM(RTRIM(?))
        """
        tablename = self.denormalize_name(table_name)
        c = connection.exec_driver_sql(has_table_query, (tablename,))
        return c.first() is not None

    @reflection.cache
    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        has_sequence_query = """
            SELECT 1 AS has_sequence 
            FROM rdb$generators
            WHERE rdb$generator_name = LTRIM(RTRIM(?))
        """
        sequencename = self.denormalize_name(sequence_name)
        c = connection.exec_driver_sql(has_sequence_query, (sequencename,))
        return c.first() is not None

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        tables_query = """
            SELECT LTRIM(RTRIM(rdb$relation_name)) AS relation_name
                FROM rdb$relations
                WHERE 
                UPPER(LTRIM(RTRIM(rdb$relation_type))) IN ('PERSISTENT')
                AND COALESCE(rdb$system_flag, 0) = 0
            ORDER BY 1
        """

        return [
            self.normalize_name(row.relation_name)
            for row in connection.exec_driver_sql(tables_query)
        ]

    @reflection.cache
    def get_temp_table_names(self, connection, schema=None, **kw):
        temp_tables_query = """
            SELECT LTRIM(RTRIM(rdb$relation_name)) AS relation_name
            FROM rdb$relations
            WHERE rdb$relation_type IN ('GLOBAL_TEMPORARY_PRESERVE', 'GLOBAL_TEMPORARY_DELETE')
              AND COALESCE(rdb$system_flag, 0) = 0
            ORDER BY 1
        """
        return [
            self.normalize_name(row.relation_name)
            for row in connection.exec_driver_sql(temp_tables_query)
        ]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        views_query = """
            SELECT LTRIM(RTRIM(rdb$relation_name)) AS relation_name
            FROM rdb$relations
            WHERE rdb$relation_type IN ('VIEW')
              AND COALESCE(rdb$system_flag, 0) = 0
            ORDER BY 1
        """
        return [
            self.normalize_name(row.relation_name)
            for row in connection.exec_driver_sql(views_query)
        ]

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
        sequences_query = """
            SELECT LTRIM(RTRIM(rdb$generator_name)) AS generator_name
            FROM rdb$generators
            WHERE COALESCE(rdb$system_flag, 0) = 0
        """
        # Do not need ORDER BY
        return [
            self.normalize_name(row.generator_name)
            for row in connection.exec_driver_sql(sequences_query)
        ]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        view_query = """
            SELECT rdb$view_source AS view_source
            FROM rdb$relations
            WHERE rdb$relation_type IN ('VIEW')
              AND rdb$relation_name = ?
        """
        viewname = self.denormalize_name(view_name)
        c = connection.exec_driver_sql(view_query, (viewname,))
        row = c.fetchone()
        if row:
            return row.view_source

        raise exc.NoSuchTableError(view_name)

    @reflection.cache
    def get_columns(  # noqa: C901
            self, connection, table_name, schema=None, **kw
    ):
        columns_query = """
            SELECT LTRIM(RTRIM(rf.rdb$field_name)) AS field_name,
                   COALESCE(rf.rdb$null_flag, f.rdb$null_flag) AS null_flag,
                   LTRIM(RTRIM(t.rdb$type_name)) AS field_type,
                   f.rdb$field_length / COALESCE(cs.rdb$bytes_per_character, 1) AS field_length,
                   f.rdb$field_precision AS field_precision,
                   f.rdb$field_scale * -1 AS field_scale,
                   f.rdb$field_sub_type AS field_sub_type,
                   f.rdb$segment_length AS segment_length,
                   LTRIM(RTRIM(cs.rdb$character_set_name)) as character_set_name,
                   LTRIM(RTRIM(cl.rdb$collation_name)) as collation_name,
                   COALESCE(rf.rdb$default_source, f.rdb$default_source) AS default_source,
                   LTRIM(RTRIM(rf.rdb$description)) AS description,
                   f.rdb$computed_source AS computed_source
                  ,rf.rdb$identity_type AS identity_type,                      -- [fb3+]
                   g.rdb$initial_value AS initial_value,                       -- [fb3+]
                   g.rdb$generator_increment AS generator_increment            -- [fb3+]
            FROM rdb$relation_fields rf
                 JOIN rdb$fields f
                   ON f.rdb$field_name = rf.rdb$field_source
                 JOIN rdb$types t
                   ON t.rdb$type = f.rdb$field_type 
                  AND t.rdb$field_name = 'RDB$FIELD_TYPE'
                 LEFT JOIN rdb$character_sets cs
                        ON cs.rdb$character_set_id = f.rdb$character_set_id
                 LEFT JOIN rdb$collations cl
                        ON cl.rdb$collation_id = rf.rdb$collation_id
                       AND cl.rdb$character_set_id = cs.rdb$character_set_id
                 LEFT JOIN rdb$generators g                                    -- [fb3+]
                        ON g.rdb$generator_name = rf.rdb$generator_name        -- [fb3+]
            WHERE COALESCE(f.rdb$system_flag, 0) = 0
              AND rf.rdb$relation_name = LTRIM(RTRIM(?))
            ORDER BY rf.rdb$field_position
        """

        is_firebird_25 = self.server_version_info < (3,)
        has_identity_columns = not is_firebird_25
        if not has_identity_columns:
            # Firebird 2.5 doesn't have RDB$GENERATOR_NAME nor RDB$IDENTITY_TYPE in RDB$RELATION_FIELDS
            #   Remove query lines containing [fb3+]
            lines = str.splitlines(columns_query)
            filtered = filter(lambda x: "[fb3+]" not in x, lines)
            columns_query = "\r\n".join(list(filtered))

        tablename = self.denormalize_name(table_name)
        c = list(connection.exec_driver_sql(columns_query, (tablename,)))

        cols = []
        for row in c:
            orig_colname = row.field_name
            colname = self.normalize_name(orig_colname)

            # Extract data type
            colclass = self.ischema_names.get(row.field_type)
            if colclass is None:
                util.warn(
                    "Unknown type '%s' in column '%s'. Check FBDialect.ischema_names."
                    % (row.field_type, colname)
                )
                coltype = sa_types.NULLTYPE
            elif issubclass(colclass, fb_types._FBString):
                if row.character_set_name == fb_types.BINARY_CHARSET:
                    if colclass == fb_types.FBCHAR:
                        colclass = fb_types.FBBINARY
                    elif colclass == fb_types.FBVARCHAR:
                        colclass = fb_types.FBVARBINARY
                if row.character_set_name == fb_types.NATIONAL_CHARSET:
                    if colclass == fb_types.FBCHAR:
                        colclass = fb_types.FBNCHAR
                    elif colclass == fb_types.FBVARCHAR:
                        colclass = fb_types.FBNVARCHAR

                coltype = colclass(
                    length=row.field_length,
                    charset=row.character_set_name,
                    collation=row.collation_name,
                )
            elif issubclass(colclass, fb_types._FBNumeric):
                # FLOAT, DOUBLE PRECISION or DECFLOAT
                coltype = colclass(row.field_precision)
            elif issubclass(colclass, fb_types._FBInteger):
                # NUMERIC / DECIMAL types are stored as INTEGER types
                if row.field_sub_type == 0:
                    # INTEGERs
                    coltype = colclass()
                elif row.field_sub_type == 1:
                    # NUMERIC
                    coltype = fb_types.FBNUMERIC(
                        precision=row.field_precision, scale=row.field_scale
                    )
                else:
                    # DECIMAL
                    coltype = fb_types.FBDECIMAL(
                        precision=row.field_precision, scale=row.field_scale
                    )
            elif issubclass(colclass, sa_types.DateTime):
                has_timezone = "WITH TIME ZONE" in row.field_type
                coltype = colclass(timezone=has_timezone)
            elif issubclass(colclass, fb_types._FBLargeBinary):
                if row.field_sub_type == 1:
                    coltype = fb_types.FBTEXT(
                        row.segment_length,
                        row.character_set_name,
                        row.collation_name,
                    )
                else:
                    coltype = fb_types.FBBLOB(row.segment_length)
            else:
                coltype = colclass()

            # Extract default value
            defvalue = None
            if row.default_source is not None:
                # the value comes down as "DEFAULT 'value'": there may be
                # more than one whitespace around the "DEFAULT" keyword
                # and it may also be lower case
                # (see also http://tracker.firebirdsql.org/browse/CORE-356)
                defexpr = row.default_source.lstrip()
                assert defexpr[:8].rstrip().upper() == "DEFAULT", (
                        "Unrecognized default value: %s" % defexpr
                )
                defvalue = defexpr[8:].strip()
                defvalue = defvalue if defvalue != "NULL" else None

            col_d = {
                "name": colname,
                "type": coltype,
                "nullable": not bool(row.null_flag),
                "default": defvalue,
            }

            if orig_colname.lower() == orig_colname:
                col_d["quote"] = True

            if row.computed_source is not None:
                col_d["computed"] = {"sqltext": row.computed_source}

            if row.description is not None:
                col_d["comment"] = row.description

            if has_identity_columns:
                if row.identity_type is not None:
                    col_d["identity"] = {
                        "always": row.identity_type == 0,
                        "start": row.initial_value,
                        "increment": row.generator_increment,
                    }

                col_d["autoincrement"] = "identity" in col_d
            else:
                # For Firebird 2.5

                # A backend is better off not returning "autoincrement" at all,
                # instead of potentially returning "False" for an auto-incrementing
                # primary key column. (see test_autoincrement_col)
                pass

            cols.append(col_d)

        if cols:
            return cols

        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(table_name)

        return (
            reflection.ReflectionDefaults.columns()
            if self.using_sqlalchemy2
            else []
        )

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        pk_query = """
            SELECT LTRIM(RTRIM(rc.rdb$constraint_name)) AS cname, LTRIM(RTRIM(se.rdb$field_name)) AS fname
            FROM rdb$relation_constraints rc
                 JOIN rdb$index_segments se
                   ON se.rdb$index_name = rc.rdb$index_name
            WHERE rc.rdb$constraint_type = 'PRIMARY KEY'
              AND rc.rdb$relation_name = LTRIM(RTRIM(?))
            ORDER BY se.rdb$field_position
        """
        tablename = self.denormalize_name(table_name)
        c = connection.exec_driver_sql(pk_query, (tablename,))

        rows = c.fetchall()
        pkfields = (
            [self.normalize_name(r.fname) for r in rows] if rows else None
        )
        if pkfields:
            return {
                "constrained_columns": pkfields,
                "name": self.normalize_name(rows[0].cname) if rows else None,
            }

        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(table_name)

        return (
            reflection.ReflectionDefaults.pk_constraint()
            if self.using_sqlalchemy2
            else {"constrained_columns": [], "name": None}
        )

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        fk_query = """
            SELECT LTRIM(RTRIM(rc.rdb$constraint_name)) AS cname,
                   LTRIM(RTRIM(cse.rdb$field_name)) AS fname,
                   LTRIM(RTRIM(ix2.rdb$relation_name)) AS targetrname,
                   LTRIM(RTRIM(se.rdb$field_name)) AS targetfname,
                   LTRIM(RTRIM(rfc.rdb$update_rule)) AS update_rule,
                   LTRIM(RTRIM(rfc.rdb$delete_rule)) AS delete_rule
            FROM rdb$relation_constraints rc
                 JOIN rdb$ref_constraints rfc 
                   ON rfc.rdb$constraint_name = rc.rdb$constraint_name
                 JOIN rdb$indices ix1 
                   ON ix1.rdb$index_name = rc.rdb$index_name
                 JOIN rdb$indices ix2 
                   ON ix2.rdb$index_name = ix1.rdb$foreign_key
                 JOIN rdb$index_segments cse 
                   ON cse.rdb$index_name = ix1.rdb$index_name
                 JOIN rdb$index_segments se 
                   ON se.rdb$index_name = ix2.rdb$index_name
                  AND se.rdb$field_position = cse.rdb$field_position
            WHERE rc.rdb$constraint_type = 'FOREIGN KEY'
              AND rc.rdb$relation_name = LTRIM(RTRIM(?))
            ORDER BY rc.rdb$constraint_name, se.rdb$field_position
        """
        tablename = self.denormalize_name(table_name)
        c = connection.exec_driver_sql(fk_query, (tablename,))

        fks = util.defaultdict(
            lambda: {
                "name": None,
                "constrained_columns": [],
                "referred_schema": None,
                "referred_table": None,
                "referred_columns": [],
                "options": {},
            }
        )

        for row in c:
            cname = self.normalize_name(row.cname)
            fk = fks[cname]
            if not fk["name"]:
                fk["name"] = cname
                fk["referred_table"] = self.normalize_name(row.targetrname)
            fk["constrained_columns"].append(self.normalize_name(row.fname))
            fk["referred_columns"].append(self.normalize_name(row.targetfname))
            if row.update_rule not in ["NO ACTION", "RESTRICT"]:
                fk["options"]["onupdate"] = row.update_rule
            if row.delete_rule not in ["NO ACTION", "RESTRICT"]:
                fk["options"]["ondelete"] = row.delete_rule

        result = list(fks.values())
        if result:
            return result

        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(table_name)

        return (
            reflection.ReflectionDefaults.foreign_keys()
            if self.using_sqlalchemy2
            else []
        )

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        condition_source_expr = """
            LTRIM(RTRIM(SUBSTRING(ix.rdb$condition_source FROM 6 FOR CHAR_LENGTH(ix.rdb$condition_source)) - 5))
        """

        if self.server_version_info < (5,):
            # Firebird 4 and lower doesn't have RDB$CONDITION_SOURCE (for partial indices)
            condition_source_expr = "CAST(NULL AS BLOB SUB_TYPE TEXT)"

        indexes_query = f"""
            SELECT 
                LTRIM(RTRIM(ix.rdb$index_name)) AS index_name,
                ix.rdb$unique_flag AS unique_flag,
                ix.rdb$index_type AS descending_flag,
                LTRIM(RTRIM(ic.rdb$field_name)) AS field_name,
                LTRIM(RTRIM(ix.rdb$expression_source)) AS expression_source,
                CAST(NULL AS VARCHAR(255)) AS condition_source -- Use VARCHAR(255) instead of BLOB SUB_TYPE TEXT
            FROM 
                rdb$indices ix
                LEFT OUTER JOIN rdb$index_segments ic ON ic.rdb$index_name = ix.rdb$index_name
                LEFT OUTER JOIN rdb$relation_constraints rc ON rc.rdb$index_name = ix.rdb$index_name
            WHERE 
                ix.rdb$relation_name = LTRIM(RTRIM('SAMPLE_TABLE'))
                AND ix.rdb$foreign_key IS NULL
                AND (rc.rdb$constraint_type IS NULL OR rc.rdb$constraint_type <> 'PRIMARY KEY')
            ORDER BY 
                ix.rdb$index_name, ic.rdb$field_position;
        """

        tablename = self.denormalize_name(table_name)

        # Do not use connection.exec_driver_sql() here.
        #    During tests we need to commit CREATE INDEX before this query. See provision.py listener.
        c = connection.execute(
            text(indexes_query), {"relation_name": tablename}
        )

        indexes = util.defaultdict(dict)
        for row in c:
            indexrec = indexes[row.index_name]
            if "name" not in indexrec:
                indexrec["name"] = self.normalize_name(row.index_name)
                indexrec["column_names"] = []
                indexrec["unique"] = bool(row.unique_flag)
                if row.expression_source is not None:
                    expr = row.expression_source[
                           1:-1
                           ]  # Remove outermost parenthesis added by Firebird
                    indexrec["expressions"] = expr.split(EXPRESSION_SEPARATOR)
                indexrec["dialect_options"] = {
                    "firebird_descending": bool(row.descending_flag),
                    "firebird_where": row.condition_source,
                }

            indexrec["column_names"].append(
                self.normalize_name(row.field_name)
            )

        def _get_column_set(tablename):
            colqry = """
                    SELECT LTRIM(RTRIM(r.rdb$field_name)) AS fname
                    FROM rdb$relation_fields r
                    WHERE r.rdb$relation_name = LTRIM(RTRIM(?))
                """
            return {
                self.normalize_name(row.fname)
                for row in connection.exec_driver_sql(colqry, (tablename,))
            }

        def _adjust_column_names_for_expressions(result, tablename):
            # Identify which expression elements are columns
            colset = _get_column_set(tablename)
            for i in result:
                expr = i.get("expressions")
                if expr is not None:
                    i["column_names"] = [
                        x if self.normalize_name(x) in colset else None
                        for x in expr
                    ]
            return result

        result = list(indexes.values())
        if result:
            return _adjust_column_names_for_expressions(result, tablename)

        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(table_name)

        return (
            reflection.ReflectionDefaults.indexes()
            if self.using_sqlalchemy2
            else []
        )

    @reflection.cache
    def get_unique_constraints(
            self, connection, table_name, schema=None, **kw
    ):
        unique_constraints_query = """
            SELECT LTRIM(RTRIM(rc.rdb$constraint_name)) AS cname,
                   LTRIM(RTRIM(se.rdb$field_name)) AS column_name
            FROM rdb$index_segments se
                 JOIN rdb$relation_constraints rc
                   ON rc.rdb$index_name = se.rdb$index_name
                 JOIN rdb$relations r
                   ON r.rdb$relation_name = rc.rdb$relation_name
                  AND COALESCE(r.rdb$system_flag, 0) = 0
            WHERE rc.rdb$constraint_type = 'UNIQUE'
              AND r.rdb$relation_name = LTRIM(RTRIM(?))
            ORDER BY rc.rdb$constraint_name, se.rdb$field_position
        """
        tablename = self.denormalize_name(table_name)
        c = connection.exec_driver_sql(unique_constraints_query, (tablename,))

        ucs = util.defaultdict(lambda: {"name": None, "column_names": []})

        for row in c:
            cname = self.normalize_name(row.cname)
            cc = ucs[cname]
            if not cc["name"]:
                cc["name"] = cname
            cc["column_names"].append(self.normalize_name(row.column_name))

        result = list(ucs.values())
        if result:
            return result

        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(table_name)

        return (
            reflection.ReflectionDefaults.unique_constraints()
            if self.using_sqlalchemy2
            else []
        )

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        table_comment_query = """
            SELECT LTRIM(RTRIM(rdb$description)) AS comment
            FROM rdb$relations
            WHERE rdb$relation_name = LTRIM(RTRIM(?))
        """
        tablename = self.denormalize_name(table_name)
        c = connection.exec_driver_sql(table_comment_query, (tablename,))

        row = c.fetchone()
        if row:
            return {"text": row[0]}

        raise exc.NoSuchTableError(table_name)

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        check_constraints_query = """
            SELECT LTRIM(RTRIM(rc.rdb$constraint_name)) AS cname,
                   SUBSTR(tr.rdb$trigger_source, 8, STRLEN(tr.rdb$trigger_source) - 7) AS sqltext
            FROM rdb$relation_constraints rc
                 JOIN rdb$check_constraints ck ON ck.rdb$constraint_name = rc.rdb$constraint_name
                 JOIN rdb$triggers tr ON tr.rdb$trigger_name = ck.rdb$trigger_name
            WHERE rc.rdb$constraint_type = 'CHECK'
                  AND rc.rdb$relation_name = LTRIM(RTRIM(?))
                  AND tr.rdb$trigger_type = 1
            ORDER BY 1
        """
        tablename = self.denormalize_name(table_name)
        c = connection.exec_driver_sql(check_constraints_query, (tablename,))

        ccs = util.defaultdict(
            lambda: {
                "name": None,
                "sqltext": None,
            }
        )

        for row in c:
            cname = self.normalize_name(row.cname)
            cc = ccs[cname]
            if not cc["name"]:
                cc["name"] = cname
                cc["sqltext"] = row.sqltext

        result = list(ccs.values())
        if result:
            return result

        if not self.has_table(connection, table_name, schema):
            raise exc.NoSuchTableError(table_name)

        return (
            reflection.ReflectionDefaults.check_constraints()
            if self.using_sqlalchemy2
            else []
        )

    @reflection.cache
    def _load_domains(self, connection, schema=None, **kw):
        domains_query = """
            SELECT LTRIM(RTRIM(f.rdb$field_name)) AS fname,
                   f.rdb$null_flag AS null_flag,
                   NULLIF(LTRIM(RTRIM(SUBSTRING(f.rdb$default_source FROM 8 FOR CHAR_LENGTH(f.rdb$default_source)) - 7)), 'NULL') fdefault,
                   LTRIM(RTRIM(SUBSTRING(f.rdb$validation_source FROM 8 FOR CHAR_LENGTH(f.rdb$validation_source)) - 8)) fcheck,
                   LTRIM(RTRIM(f.rdb$description)) fcomment
            FROM rdb$fields f
            WHERE COALESCE(f.rdb$system_flag, 0) = 0
              AND f.rdb$field_name NOT STARTING WITH 'RDB$'
            ORDER BY 1
        """
        result = connection.exec_driver_sql(domains_query)
        return [
            {
                "name": self.normalize_name(row["fname"]),
                "nullable": not bool(row.null_flag),
                "default": row["fdefault"],
                "check": row["fcheck"],
                "comment": row["fcomment"],
            }
            for row in result.mappings()
        ]


# Register the custom dialect
registry.register("interbase", __name__, "IBDialect")
