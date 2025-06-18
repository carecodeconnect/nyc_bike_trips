from behave import given, when, then

@given('We have trip data CSV file')
def step_impl(context):
    # check for CSV file
    pass

@when('We load the data into the application')
def step_impl(context):
    # load data
    pass

@then('Data should be available for further processing')
def step_imp(context):
    # check data is loaded
    pass