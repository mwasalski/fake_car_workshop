# Build from the REPO ROOT (the image needs reference_data.py / table_schemas.py):
#   docker build -f simulator/Dockerfile -t car-workshop-sim .
#   docker run -p 8000:1289 -v "$PWD/data:/data" -e HOST_DATA_DIR="$PWD/data" car-workshop-sim
# then open http://localhost:8000
# (HOST_DATA_DIR is optional - it lets the "open data folder" button show the host path)

FROM python:3.12-slim

# links the GHCR package to the repo (shows it on the repo page, inherits README)
LABEL org.opencontainers.image.source=https://github.com/mwasalski/fake_car_workshop
LABEL org.opencontainers.image.description="fake_car_workshop local ERP simulator - generate & preview the synthetic dataset without Spark"

WORKDIR /app

COPY simulator/requirements.txt simulator/requirements.txt
RUN pip install --no-cache-dir -r simulator/requirements.txt

COPY reference_data.py table_schemas.py ./
COPY simulator/ simulator/

ENV CAR_WORKSHOP_DATA=/data
VOLUME /data
EXPOSE 1289

CMD ["uvicorn", "simulator.app:app", "--host", "0.0.0.0", "--port", "1289"]
