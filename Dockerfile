FROM python:3.11.2

COPY . .

RUN pip install -r requirements.txt

CMD ["python", "etl_script.py"]