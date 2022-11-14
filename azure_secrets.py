import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from config import servicebus_connection_key


def init():
    global _global_dict
    _global_dict = {}
    _load_config_from_keyvault()


def get_value(name: str) -> str:
    return _global_dict[name]


def _load_config_from_keyvault():
    vault_client = SecretClient(vault_url=os.environ.get('AZURE_KEY_VAULT_URL'),
                                credential=DefaultAzureCredential())

    service_bus_connection = vault_client.get_secret(servicebus_connection_key)
    _global_dict['SERVICE_BUS_CONNECTION_STR'] = service_bus_connection.value
