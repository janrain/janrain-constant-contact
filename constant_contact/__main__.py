"""This allows the package to be run as if it was a module."""
from constant_contact import app

if __name__ == '__main__':
    # launch a debug web server on localhost:5000
    app.run()
