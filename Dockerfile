# 
FROM python:3.12

# 
WORKDIR /code

# 
COPY ./requirements.txt /code/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 
COPY ./app /code/app

# Expose the FastAPI port
EXPOSE 8000

# 
CMD ["fastapi", "run", "app/main.py", "--proxy-headers", "--port", "8000"]