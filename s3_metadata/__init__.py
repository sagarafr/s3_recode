from flask import Flask


def create_app():
    app = Flask(__name__)

    from s3_metadata.bucket.bucket import BP_BUCKET
    app.register_blueprint(BP_BUCKET)

    from s3_metadata.objects.object import BP_OBJECT
    app.register_blueprint(BP_OBJECT)

    from s3_metadata.utils.commands import init_app
    init_app(app)

    app.logger.debug(f"url map {app.url_map}")

    @app.route("/ping")
    def ping():
        return {
            "response": "pong"
        }

    return app
