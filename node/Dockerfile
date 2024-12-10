FROM python:3.13-alpine

WORKDIR /app

COPY requirements.txt ./node/requirements.txt

RUN pip install --no-cache-dir -r node/requirements.txt

COPY . ./node

CMD ["uvicorn", "node.main:app", "--host", "0.0.0.0", "--port", "8000"]