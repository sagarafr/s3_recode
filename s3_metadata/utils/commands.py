import click
from flask import Flask
from flask.cli import with_appcontext

from .db import init_db


@click.command("init-s3-metadata")
@with_appcontext
def init_s3_metadata():
    init_db()
    click.echo("Initialized the database.")


def init_app(app: Flask):
    app.cli.add_command(init_s3_metadata)
