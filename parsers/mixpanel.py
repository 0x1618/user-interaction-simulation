# Author: Maksymilian Sawicz (max.sawicz@gmail.com)
# Code under MIT License
# Basically, to use my code you just need to include my name and my e-mail wherever you use this code.

import json
import re
from base64 import b64encode
from typing import List, Union

import requests

from parsers.events import Events, Schema


class MixpanelParser:
    """
    A class for interacting with the Mixpanel API to download event data and parse it into a structured format.
    """

    class DataParams:
        """
        A class for defining parameters for downloading Mixpanel event data.

        Args:
            **kwargs: Keyword arguments specifying data download parameters (e.g., from_date, to_date, limit, event, where).

        Raises:
            NotImplementedError: If an unsupported parameter is provided.
            ValueError: If a parameter fails validation checks.

        """

        def __init__(self, **kwargs) -> None:
            """
            Initialize DataParams instance with specified parameters and perform validation checks.

            Args:
                **kwargs: Keyword arguments specifying data download parameters.

            Raises:
                NotImplementedError: If an unsupported parameter is provided.
                ValueError: If a parameter fails validation checks.

            """

            self.SUPPORTED_PARAMS = (
                'from_date',
                'to_date',
                'limit',
                'event',
                'where'
            )

            DATE_REGEX = r'^(?:19|20)\d\d-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])$'

            self.PARAMS_CHECKS = {
                'from_date': {
                    'functions': ({
                            'function': lambda date: re.search(DATE_REGEX, date) is not None,
                            'error': 'from_date param should be in the following pattern: YYYY-MM-DD | You provided {user_input}'
                        },
                        {
                            'function': lambda _: bool(kwargs.get('to_date')),
                            'error': 'from_date param have to be with to_date param'
                        }
                    )
                },
                'to_date': {
                    'functions': ({
                            'function': lambda date: re.search(DATE_REGEX, date) is not None,
                            'error': 'to_date param should be in the following pattern: YYYY-MM-DD | You provided {user_input}'
                        },
                        {
                            'function': lambda _: bool(kwargs.get('from_date')),
                            'error': 'to_date param have to be with from_date param'
                        }
                    )
                },
                'limit': {
                    'functions': ({
                            'function': lambda limit: isinstance(limit, int),
                            'error': 'limit param should be integer'
                        },
                    )
                },
                'event': {
                    'functions': ({
                            'function': lambda event: isinstance(event, list),
                            'error': 'event param should be list',
                            'custom_result': lambda event: json.dumps(event, ensure_ascii=False)
                        },
                    )
                },
                'where': { # https://developer.mixpanel.com/reference/segmentation-expressions#examples
                    'functions': ()
                }
            }

            for key, value in kwargs.items():
                if key not in self.SUPPORTED_PARAMS:
                    raise NotImplementedError(f'"{key}" is not supported param. Supported params are {self.SUPPORTED_PARAMS}')
                
                for data in self.PARAMS_CHECKS[key]['functions']:
                    if data['function'](value) is not True:
                        raise ValueError(data['error'].format(user_input=value))
                    
                    if data.get('custom_result'):
                        kwargs[key] = data['custom_result'](value)
                
            self.params = kwargs
    
    def __init__(self, project_id: int, service_account_username: str, service_account_secret: str) -> None:
        """
        Initialize a MixpanelParser instance with project details.

        Args:
            project_id (int): The Mixpanel project ID.
            service_account_username (str): The username for the Mixpanel service account.
            service_account_secret (str): The secret key for the Mixpanel service account.

        """

        self.project_id = project_id

        self.service_account_username = service_account_username
        self.service_account_secret = service_account_secret

    def download_data(self, data_params: DataParams, data_location: str = 'data-eu', json_path: Union[str, None] = 'events.json') -> Events:
        """
        Download event data from Mixpanel using specified parameters and save it to a JSON file.

        Args:
            data_params (MixpanelParser.DataParams): Data parameters for downloading event data.
            data_location (str): Mixpanel data location (default is 'data-eu').
            json_path (Union[str, None]): The path to save the downloaded data as a JSON file (default is 'events.json').

        Returns:
            Events: An instance of the Events class containing parsed event data.

        """

        url = f"https://{data_location}.mixpanel.com/api/2.0/export"

        headers = {
            "accept": "application/json",
            "authorization": self._authorization_as_base64()
        }

        params = data_params.params

        params['project_id'] = self.project_id

        response = requests.get(url,
            headers=headers,
            params=params
        )

        data = self._extract_events_from_response(
            data=response.text
        )

        if json_path is not None:
            self._save_response_data_to_json(
                json_path=json_path,
                data=data
            )

        return Events(
            events=data,
            inital_parser='mixpanel'
        )

    def _authorization_as_base64(self) -> str:
        """
        Encode Mixpanel service account credentials as base64 for authorization.

        Returns:
            str: The authorization header as a base64-encoded string.

        """

        string_to_encode = f'{self.service_account_username}:{self.service_account_secret}'.encode()
        base64_string = b64encode(string_to_encode).decode()

        return f'Basic {base64_string}'
    
    def _save_response_data_to_json(self, json_path: str, data: str) -> None:
        """
        Save downloaded event data as a JSON file.

        Args:
            json_path (str): The path to save the JSON file.
            data (str): The event data to be saved.

        """

        with open(json_path, 'w', encoding='utf-8') as f:
            f.write(
                json.dumps(data, indent=4)
            )

    def _extract_events_from_response(self, data: str) -> List[dict]:
        """
        Extract events from the Mixpanel API response.

        Args:
            data (str): The raw API response data.

        Returns:
            List[dict]: A list of dictionaries representing individual events.

        """

        return [
            json.loads(event) for event in data.splitlines()
        ]

if __name__ == "__main__":
    m = MixpanelParser(
        service_account_secret='XXXX',
        service_account_username='XXXX',
        project_id='XXXX'
    )

    data_params = MixpanelParser.DataParams(
        from_date='2023-09-21',
        to_date='2023-09-22',
        where='properties["$distinct_id"] == "XXXX-XXXX-XXXX-XXXX"'
    )

    events = m.download_data(
        data_params=data_params
    )

    s = Schema('reproductive', 'dimension', 'scrollTop', 'mousePosition', 'time', 'location')

    print(events.parse(s))