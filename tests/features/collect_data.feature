Feature: Collect trips data

    Scenario: Load trip data from a CSV file
        Given I have trip data CSV file
        When I load the data into the application
        Then the data should be available for processing