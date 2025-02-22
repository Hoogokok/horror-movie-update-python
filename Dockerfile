FROM python:3.12.3
LABEL authors="ryun"
WORKDIR /app
#
RUN apt-get update && apt-get install -y wget unzip

# Google Chrome 설치
RUN wget -O /tmp/google-chrome-stable_current_amd64.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i /tmp/google-chrome-stable_current_amd64.deb; apt-get -fy install

# ChromeDriver 다운로드 및 설치
RUN wget -O /tmp/chromedriver-linux64.zip https://storage.googleapis.com/chrome-for-testing-public/126.0.6478.62/linux64/chromedriver-linux64.zip
# Unzip the file
RUN unzip /tmp/chromedriver-linux64.zip -d /tmp
# Move the file to the correct path
RUN mv /tmp/chromedriver-linux64/chromedriver /usr/bin/chromedriver
RUN chmod +x /usr/bin/chromedriver

# 환경변수 설정
COPY .env* .

# Python dependencies 설치
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# Clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# 애플리케이션 실행
CMD ["python", "update_scheduler.py"]