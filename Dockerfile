FROM python:3.6.9-stretch
RUN apt-get update && apt-get install -y tdsodbc unixodbc-dev && apt-get install -y libsasl2-dev gcc python-dev libsasl2-2 libsasl2-modules-gssapi-mit \
 && apt install unixodbc-bin -y  \
 && apt-get clean -y
MAINTAINER Andrew Konstantinov
COPY requirements.txt /add
COPY dpmi_utils.py /add
COPY prtg_final.py /add
WORKDIR /app
ADD  . /app
RUN pip install -r requirements.txt
CMD ["python3","prtg_final.py"]
