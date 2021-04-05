FROM python:3
WORKDIR /home/ec2-user
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
