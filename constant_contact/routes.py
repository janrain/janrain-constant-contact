"""App routes."""
from .actions import sync

def add_routes(app):
    app.add_url_rule('/', 'root', lambda: 'ok')
    app.add_url_rule('/sync', 'sync', sync, methods=['POST'])
