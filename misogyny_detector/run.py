# run.py

from app import create_app

application = create_app()  # Beanstalk 會找這個名字

if __name__ == '__main__':
    application.run(debug=True)