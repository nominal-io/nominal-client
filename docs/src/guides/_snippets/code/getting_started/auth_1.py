from nominal.core import NominalClient

client = NominalClient.from_profile("default")

# Get details about the currently logged-in user to validate authentication
# Will display an object like: `User(display_name='your_email@your_company.com', ...)`
print(client.get_user())
