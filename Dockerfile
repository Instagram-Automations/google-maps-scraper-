FROM apify/actor-python:3.11

COPY . ./

RUN playwright install --with-deps

CMD ["python", "main.py"]
