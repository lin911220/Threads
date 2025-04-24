# app/__init__.py

from flask import Flask

def create_app():
    app = Flask(__name__)

    # 註冊藍圖
    from app.controllers.main import main_bp
    app.register_blueprint(main_bp)
    
    return app
