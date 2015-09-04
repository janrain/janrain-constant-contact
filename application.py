"""Entry point for beanstalk to load the flask application."""
# must be named application for beanstalk to find it automatically
from constant_contact import app as application
