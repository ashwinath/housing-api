FROM python:3.10-alpine
LABEL org.opencontainers.image.authors="ashwinath@hotmail.com"
LABEL org.opencontainers.image.source https://github.com/ashwinath/housing-api

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./main.py" ]
