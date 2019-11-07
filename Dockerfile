FROM python:3.6.4

WORKDIR /etl/

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "python", "/etl/main.py" ]
CMD []
