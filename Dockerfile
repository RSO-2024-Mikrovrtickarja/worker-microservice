FROM python:3.12.8-bullseye

WORKDIR /app
COPY . /app

RUN curl -sSL https://install.python-poetry.org | python3 - \
    && /root/.local/bin/poetry install --no-root

CMD ["/root/.local/bin/poetry", "run", "python", "main.py"]
