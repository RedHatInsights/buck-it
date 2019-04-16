FROM python:3.6
WORKDIR /usr/src/app
COPY . .
RUN pip install pipenv
RUN pipenv install
RUN pipenv run pip freeze > requirements.txt
RUN pip install -r requirements.txt
CMD ["python", "./app.py"]
