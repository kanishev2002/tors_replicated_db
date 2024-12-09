FROM python:3.13-alpine

WORKDIR /app

COPY . ./node

WORKDIR /app/node

RUN pip install -r requirements.txt

CMD ["fastapi", "run", "main.py"]
