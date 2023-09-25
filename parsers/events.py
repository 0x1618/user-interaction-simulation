# Author: Maksymilian Sawicz (max.sawicz@gmail.com)
# Code under MIT License
# Basically, to use my code you just need to include my name and my e-mail wherever you use this code.

import json
from typing import Any, Dict


class Variables:
    """
    Class containing constant variables and configurations for the application.

    Attributes:
        ALLOWED_PARSERS (tuple): A tuple of allowed parser names.
    """

    ALLOWED_PARSERS = (
        'mixpanel',
    )

class Schema:
    """
    Class representing the schema for parsing event data.
    """

    def __init__(self, reproductive_data_key: str = None, dimension_key: str = None, scroll_top_key: str = None, mouse_position_key: str = None, time_key: str = None, page_key: str = None, query_key: str = None) -> None:
        """
        Initialize a Schema instance with schema keys.

        Args:
            reproductive_data_key (str): The key for reproductive data in event properties.
            dimension_key (str): The key for dimension data in event properties.
            scroll_top_key (str): The key for scroll top data in event properties.
            mouse_position_key (str): The key for mouse position data in event properties.
            time_key (str): The key for time data in event properties.
            page_key (str): The key for page data in event properties.
            query_key (str): The key for query data in event properties.

        Raises:
            ValueError: If all arguments are set to None.
        """

        self.reproductive_data_key = reproductive_data_key
        self.dimension_key = dimension_key
        self.scroll_top_key = scroll_top_key
        self.mouse_position_key = mouse_position_key

        self.time_key = time_key
        self.page_key = page_key
        self.query_key = query_key

        if all((not value for value in self.__dict__.values())):
            raise ValueError('All arguments cannot be None')
        
    def return_schema(self) -> dict:
        """
        Return a dictionary representing the non-None attributes of the schema.

        Returns:
            dict: A dictionary containing schema attributes with non-None values.
        """

        schema = {key: value for key, value in self.__dict__.items() if value is not None}
        schema.pop('reproductive_data_key')

        return schema

class Events:
    '''
    Class for parsing and managing event data.
    '''

    def __init__(self, events: dict, schema: Schema = None, inital_parser: str = 'mixpanel') -> None:
        """
        Initialize an Events instance with event data, schema, and initial parser.

        Args:
            events (dict): The event data to be parsed.
            schema (Schema): The schema for parsing the event data.
            inital_parser (str): The name of the initial parser to use.

        Raises:
            NotImplementedError: If the specified initial parser is not supported.
        """

        self.events = events
        self.inital_parser = inital_parser
        self.schema = schema

        if inital_parser not in Variables.ALLOWED_PARSERS:
            raise NotImplementedError(f'Inital parser "{inital_parser}" is not supported. Inital parser that are supported: {Variables.ALLOWED_PARSERS}')
        
        self.PARSERS_AND_THEIR_FUNCTIONS = {
            'mixpanel': Events.__mixpanel_inital_parser
        }

        self.parsed_events = {}

    def parse(self, schema: Schema) -> Any:
        """
        Parse the event data using the provided schema.

        Args:
            schema (Schema): The schema for parsing the event data.

        Returns:
            Any: The parsed event data.
        """

        self.schema = schema

        return self.__perfom_parsing()

    @classmethod
    def parse_from_file(cls, json_path: str, schema: Schema, inital_parser: str = 'mixpanel'):
        """
        Parse event data from a JSON file using the provided schema and initial parser.

        Args:
            json_path (str): The path to the JSON file containing event data.
            schema (Schema): The schema for parsing the event data.
            inital_parser (str): The name of the initial parser to use.

        Returns:
            Events: An instance of the Events class with parsed event data.

        Raises:
            ValueError: If the JSON file does not contain valid JSON data.
        """

        with open(json_path, 'r', encoding='utf-8') as f:
            _events = f.read()
            
            try:
                _events = json.loads(_events)
            except json.decoder.JSONDecodeError:
                raise ValueError(f'File with the path "{json_path}" doesn\'t contain JSON data')

        instance = cls(
            events=_events,
            schema=schema,
            inital_parser=inital_parser
        )

        instance.__perfom_parsing()

        return instance

    def __mixpanel_inital_parser(self) -> Dict:
        """
        Parse event data using the Mixpanel initial parser.

        Returns:
            Dict: The parsed event data.
        """

        valid_event = 0

        for event in self.events:
            properties = event['properties']

            parsed_event = {'name': event['event']}

            schema = self.schema.return_schema()

            if (reproductive := properties.get(self.schema.reproductive_data_key)):
                for key, value in schema.items():
                    parsed_event[key.replace('_key', '')] = reproductive.get(value)

                parsed_event['time'] = properties.get(schema.get('time_key'))
                parsed_event['page'] = properties.get(schema.get('page_key'))

                if schema.get('query_key') and properties.get(schema.get('query_key')):
                    parsed_event['page'] = parsed_event['page'] + properties.get(schema.get('query_key'))
                
                valid_event += 1

                self.parsed_events[valid_event] = parsed_event

                parsed_event.pop('query')
                
                continue
            else:
                for key, value in schema.items():
                    parsed_event[key.replace('_key', '')] = properties.get(value)

                parsed_event['time'] = properties.get(schema.get('time_key'))
                parsed_event['page'] = properties.get(schema.get('page_key'))
                
                if schema.get('query_key'):
                    parsed_event['page'] = parsed_event['page'] + properties.get(schema.get('query_key'))
                
                valid_event += 1

                parsed_event.pop('query')

                self.parsed_events[valid_event] = parsed_event
                
                continue
        
        return self.parsed_events

    def __perfom_parsing(self) -> Any:
        """
        Perform event data parsing using the specified initial parser.

        Returns:
            Any: The parsed event data.

        """

        return self.PARSERS_AND_THEIR_FUNCTIONS[self.inital_parser](self)

if __name__ == "__main__":
    s = Schema('reproductive', 'dimension', 'scrollTop', 'mousePosition', 'time', 'location', 'searchArgs')
    e = Events.parse_from_file('events.json', s)