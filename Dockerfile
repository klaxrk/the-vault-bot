FROM python:3.11-slim
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py .
RUN mkdir -p /app/data
VOLUME /app/data
CMD ["python", "-u", "bot.py"]
