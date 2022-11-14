FROM nvidia/cuda:11.8.0-runtime-ubuntu22.04

WORKDIR /usr/src/app

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y python3.10 python3-pip libsm6 libxext6 libxrender-dev libglib2.0-dev libgl1-mesa-glx
RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

COPY . /usr/src/app/

ENTRYPOINT ["python3.10", "main.py"]