# Author: Maksymilian Sawicz (max.sawicz@gmail.com)
# Code under MIT License
# Basically, to use my code you just need to include my name and my e-mail wherever you use this code.

from time import sleep
from typing import Callable, Union

from selenium import webdriver
from selenium.common.exceptions import JavascriptException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.actions.action_builder import ActionBuilder

from parsers.events import Events


class Controls:
    """
    Utility class for simulating user actions in a Selenium WebDriver.
    """

    @staticmethod
    def simulate_click(driver, x, y) -> None:
        """
        Simulate a mouse click at the specified coordinates (x, y) on the web page.

        Args:
            driver: The Selenium WebDriver instance.
            x (int): The x-coordinate of the click position.
            y (int): The y-coordinate of the click position.
        """

        action = ActionBuilder(driver)
        action.pointer_action.move_to_location(x, y)
        action.pointer_action.click()
        action.perform()

    @staticmethod
    def simulate_scroll(driver, pixels) -> None:
        """
        Simulate scrolling the web page to a given number of pixels.

        Args:
            driver: The Selenium WebDriver instance.
            pixels (int): The number of pixels to scroll.
        """
        
        try:
            driver.execute_script(f"window.scrollTo(0, {pixels});")
        except JavascriptException:
            print('Simulating scroll failed')

class UserSimulation:
    """
    Class for simulating user interactions with a web page using Selenium WebDriver.
    """

    def __init__(self, parsed_events: Events) -> None:
        """
        Initialize a UserSimulation instance with parsed events and configure WebDriver options.

        Args:
            parsed_events (Events): An instance of the parsed events class containing user interaction data.
        """

        self.events = parsed_events
        self.first_event = next(iter(self.events.items()))[1]
        
        options = Options()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        emulation = {
            "deviceMetrics": {
                "width": self.first_event['dimension'][0],
                "height": self.first_event['dimension'][1],
                "pixelRatio": 3.0
            },
            "userAgent": "Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 5 Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19",
            "clientHints": {"platform": "Android", "mobile": True}
        }

        options.add_experimental_option("mobileEmulation", emulation)

        self.driver = webdriver.Chrome(options=options)


    def perform_simulation(self, static_sleep_time: Union[int, float, None] = None, execute_function: Union[None, Callable] = None):
        """
        Perform the simulation of user interactions based on parsed events.

        Args:
            static_sleep_time (Union[int, float, None]): Static sleep time between events (seconds). If None then function will use time between each event.
            execute_function (Union[None, Callable]): A custom function to execute during each event.
        """

        last_timestamp = self.first_event['time']
        last_location = None

        for n, event in self.events.items():
            if static_sleep_time is None:
                next_event_in = event['time'] - last_timestamp

                if next_event_in < 0:
                    next_event_in = 3

                sleep(next_event_in)
            else:
                sleep(static_sleep_time)

            if execute_function is not None and callable(execute_function):
                execute_function(
                    n=n,
                    event=event,
                    events=self.events,
                    driver=self.driver,
                    last_location=last_location,
                    last_timestamp=last_timestamp
                )
            else:
                if last_location != event['page']:
                    self.driver.get(event['page'])
            
            if event.get('scroll_top'):
                Controls.simulate_scroll(
                    driver=self.driver,
                    pixels=event['scroll_top']
                )

            if event.get('mouse_position'):
                Controls.simulate_click(
                    driver=self.driver,
                    x=event['mouse_position'][0],
                    y=event['mouse_position'][1]
                )

            last_timestamp = event['time']
            last_location = event['page']
