from sqlalchemy.dialects import registry
from sqlalchemy_firebird.base import FBDialect

# Define your custom Firebird dialect
class FBDialect_interbase(FBDialect):
    driver = 'interbase'

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
        # Example: set server version info based on actual retrieval method
        self.server_version_info = (2, 5, 0)  # Replace with actual logic to fetch server version
        # Call super method after setting server version info
        if self.server_version_info < (3,):
            # Firebird 2.5
            from sqlalchemy_firebird.fb_info25 import MAX_IDENTIFIER_LENGTH, RESERVED_WORDS

            self.supports_identity_columns = False
            self.supports_native_boolean = False
        elif self.server_version_info < (4,):
            # Firebird 3.0
            from sqlalchemy_firebird.fb_info30 import MAX_IDENTIFIER_LENGTH, RESERVED_WORDS
        else:
            # Firebird 4.0 or higher
            from sqlalchemy_firebird.fb_info40 import MAX_IDENTIFIER_LENGTH, RESERVED_WORDS

        self.max_identifier_length = MAX_IDENTIFIER_LENGTH
        self.preparer.reserved_words = RESERVED_WORDS

# Register the custom dialect
registry.register("firebird.interbase", __name__, "FBDialect_interbase")


