FROM python:3.12.10-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY simulate.py fixture.py verify.py ./
CMD ["python", "simulate.py", "--tps", "20"]
