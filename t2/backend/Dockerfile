FROM python:3.14
ARG SERVICE
WORKDIR /app
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ${SERVICE}/app .
CMD ["fastapi", "run", "main.py", "--port", "8000"]