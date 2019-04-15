FROM python:3.7
WORKDIR /usr/src/app
COPY . .
RUN pipenv install
CMD ["pipenv", "run", "python", "./app.py"]
