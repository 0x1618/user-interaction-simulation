#########

# EXAMPLE FILE

#########

# Author: Maksymilian Sawicz (max.sawicz@gmail.com)
# Code under MIT License
# Basically, to use my code you just need to include my name and my e-mail wherever you use this code.

from navigators.selenium_navigator import UserSimulation
from parsers.events import Events, Schema
from parsers.mixpanel import MixpanelParser

s = Schema('reproductive', 'dimension', 'scrollTop', 'mousePosition', 'time', 'location', 'searchArgs')

def __download_events():
    m = MixpanelParser(
        service_account_secret='XXXX',
        service_account_username='XXXX',
        project_id='XXXX'
    )

    data_params = MixpanelParser.DataParams(
        from_date='2023-09-23',
        to_date='2023-09-23',
        where='properties["$distinct_id"] == "XXXX-XXXX-XXXX-XXXX-XXXX"',
        event=['Page viewed', 'Button clicked', 'Other event', 'Other Other event']
    )

    events = m.download_data(
        data_params=data_params
    )

    return events.parse(schema=s)

def __use_saved_events():
    return Events.parse_from_file(json_path='parsers/events.json', schema=s, inital_parser='mixpanel')

def function_that_i_want_to_execute_inside_loop(**kwargs):
    # MUST CONTAIN **KWARGS
    # | **kwargs contains following variables:
    #                n
    #                event
    #                events
    #                driver
    #                last_location
    #                last_timestamp

    n = kwargs['n']

    driver = kwargs['driver']

    event = kwargs['event']
    events = kwargs['events']

    last_location = kwargs['last_location']

    print(event)

    if last_location != event['page'] or (
            last_location != event['page'] and
            events.get(n + 1) and
            (event['name'] == 'Button clicked' and events.get(n + 1)['name'] == 'Page viewed')
        ):
        driver.get(event['page'])

# events = __download_events() or events = __use_saved_events()

events = __download_events()

us = UserSimulation(parsed_events=events)
us.perform_simulation(
    static_sleep_time=None,
    execute_function=function_that_i_want_to_execute_inside_loop
)
