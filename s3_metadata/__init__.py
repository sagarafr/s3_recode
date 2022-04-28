from flask import Flask


def create_app():
    app = Flask(__name__)

    from s3_metadata.bucket.bucket import BP_BUCKET
    app.register_blueprint(BP_BUCKET)

    from s3_metadata.utils.commands import init_app
    init_app(app)

    @app.route("/ping")
    def ping():
        return {
            "response": "pong"
        }

    return app
