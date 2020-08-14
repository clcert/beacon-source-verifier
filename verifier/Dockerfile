FROM python:3

WORKDIR /app

ENV PYTHONBUFFERED=TRUE

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "./main.py" ]