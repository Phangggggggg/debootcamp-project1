FROM python:3.9

WORKDIR /app


RUN apt-get update && \
    apt-get install -y \
    build-essential \
    gdal-bin \
    libgdal-dev \
    libproj-dev

COPY . /app

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

CMD ["python", "-m", "etl_project.pipeline"]