import os

os.environ["environment"] = 'Test'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

environment = os.environ['environment']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
current_path = os.getcwd()
app_name = 'USA prescriber research report'
staging_dim_city = current_path + '\..\staging\dimention_city'
facts = current_path + '\..\staging\\facts'
