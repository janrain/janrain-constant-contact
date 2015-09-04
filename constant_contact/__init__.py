"""Flask application setup."""
import flask
from .config import apply_configuration
from .routes import add_routes

app = flask.Flask(__name__)
apply_configuration(app)
add_routes(app)
