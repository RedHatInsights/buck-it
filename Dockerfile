FROM python:3.6
WORKDIR /usr/src/app
COPY . .
RUN pip install pipenv
RUN pipenv install
CMD ["pipenv", "run", "python", "./app.py"]
