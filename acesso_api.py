from getpass import getpass
import json
import requests

token = getpass()

#A url base é comum a todos os métodos da API.

base_url = f'https://api.telegram.org/bot{token}'

#O método getMe retorna informações sobre o bot.

response = requests.get(url=f'{base_url}/getMe')

# Retorna as mensagens captadas pelo bot.

response = requests.get(url=f'{base_url}/getUpdates')




