FROM python:3.9

RUN mkdir /var/app
WORKDIR /var/app
COPY requirements.txt .
COPY collect/main.py .
RUN pip install -r requirements.txt
CMD [ "python", "main.py" ]
