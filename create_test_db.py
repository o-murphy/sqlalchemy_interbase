import os
import interbase

# Get the current working directory
current_dir = os.getcwd()

# Define the relative path and construct the full DSN
relative_path = 'TEST.DB'
dsn = f'localhost/3051:{os.path.join(current_dir, relative_path)}'

# Define the connection parameters
user = 'sysdba'
password = 'masterkey'
charset = 'WIN1252'  # Specify the default charset

# Create the database
connection = interbase.create_database(
    dsn=dsn,
    user=user,
    password=password,
    page_size=4096,  # Optional parameter
    charset=charset,  # Set the default charset
)

# Connect database
# connection = interbase.connect(
#     dsn=dsn,
#     user=user,
#     password=password,
#     charset=charset,  # Set the default charset
# )


print('Database created successfully at', dsn)

# Close the connection
connection.close()
