from nominal.core import NominalClient

# Get an instance of the client using provided credentials
client = NominalClient.from_token("<insert api key>")

# Get details about the currently logged-in user to validate authentication
# Will display an object like: `User(display_name='your_email@your_company.com', ...)`
print(client.get_user())
