FROM python:3.11-slim

WORKDIR /app

RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

COPY --chown=user requirements.txt $HOME/app/
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY --chown=user . $HOME/app

EXPOSE 7860

CMD ["uvicorn", "app_store_api:app", "--host", "0.0.0.0", "--port", "7860"]
