# 스푸키 타운 영화 정보 업데이트 시스템
## 1. 개요
- 스푸키 타운 영화 정보 업데이트 시스템은 스푸키 타운의 영화 정보를 업데이트 합니다.
- 셀레니움의 웹 스크래핑을 이용하여 영화 정보를 수집합니다.
- 수집한 정보를 데이터베이스에 저장합니다.
- 영화 정보 수집은 매주 한 번 수행됩니다.
- 도커를 이용하여 배포합니다.

## 2. 기능
- 영화 정보 수집
  - 상영 중인 영화 정보
  - 상영 종료된 영화 정보
  - 개봉 예정 영화 정보
  - 넷플릭스 만료 예정 영화 정보
- 영화관 정보 업데이트
- 영화 스트리밍 공급자 정보 업데이트
- 데이터베이스 저장

## 3. 개발 환경
- Python 3.7+
- Selenium
- PostgreSQL
- Docker
- Supabase
- Tweepy

## 4. 주요 의존성
- selenium==4.21.0
- supabase==2.5.1
- tweepy~=4.14.0
- webdriver-manager==3.8.6

## 5. 스케줄러
- 스케줄러는 매주 한 번 다음 작업을 수행합니다:
  1. 넷플릭스 만료 예정 영화 정보 업데이트
  2. 모든 영화관의 영화 정보 수집
  3. 영화관 정보 업데이트
  4. 상영 종료된 영화 정보 업데이트
  5. 개봉 예정 영화 정보 업데이트
  6. 영화 스트리밍 공급자 정보 업데이트
