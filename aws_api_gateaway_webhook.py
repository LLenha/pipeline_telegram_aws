# url gerada na variável aws_api_gateway_url
from getpass import getpass

aws_api_gateway_url = getpass()

# setWebhook

response = requests.get(url=f'{base_url}/setWebhook?url={aws_api_gateway_url}')

# getWebhookInfo

response = requests.get(url=f'{base_url}/getWebhookInfo')