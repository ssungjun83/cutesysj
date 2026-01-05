# 카카오톡 대화 웹 게시 (비밀번호)

이 폴더에 있는 카카오톡 대화 저장 `.txt` 파일을 가져와서, **비밀번호 로그인 후** 웹에서 같은 방식으로 볼 수 있는 작은 서버입니다.  
대화는 계속 추가(가져오기)할 수 있고, **중복(날짜+메시지 내용이 같음)** 은 자동 제외됩니다. *(이름이 달라도 제외됨)*

## 내가 반드시 해야 할 것(최소)

1. **Python 3.11 이상 설치**
2. 이 폴더에서 PowerShell로 `.\setup.ps1` 실행 (처음 1번)
3. `.\run.ps1` 실행
4. 브라우저에서 `http://127.0.0.1:8000` 접속 → 비밀번호 입력 → `가져오기`로 `.txt` 올리기

## (선택) 비밀번호 없이 접속하기

운영 URL을 공개하면 누구나 볼 수 있으니 권장하지 않습니다.

- 환경변수에 `CHAT_APP_DISABLE_AUTH=1` 를 설정하면 로그인(비밀번호)을 끕니다.
- Render에서는 서비스의 Environment에 `CHAT_APP_DISABLE_AUTH=1` 추가 후 재배포하면 됩니다.

## 다른 사람이 인터넷에서 보게 하기 (추천: 포트포워딩/서버 없이)

- PowerShell에서 `.\share.ps1` 실행
- 출력에 나오는 `https://....trycloudflare.com` 주소를 공유하면 됩니다. (접속 시 비밀번호 필요)
- 단, 내 PC가 켜져 있고 `share.ps1`가 실행 중일 때만 접속 가능합니다.

## PC 없이 “외부 서버”에서 상시 운영하기 (GitHub 사용)

GitHub는 **코드를 올리는 곳**이고, 실제로 24시간 실행되는 “서버”는 Render/Fly/VPS 같은 호스팅이 필요합니다.  
가장 쉬운 흐름은 **GitHub에 올리고 → 호스팅이 GitHub에서 자동 배포**하는 방식입니다.

내가 해야 할 것(최소)
1. 이 폴더를 GitHub 저장소로 올리기
2. 호스팅에서 “Docker로 배포” 선택 (Render 추천)
3. 호스팅의 환경변수에 `CHAT_APP_PASSWORD_HASH` 설정 (비밀번호 평문이 아니라 해시)
4. (중요) DB가 사라지지 않게 “영구 디스크/볼륨”을 붙이고 `CHAT_APP_DATA_DIR`를 그 경로로 설정 (Render는 `render.yaml`로 자동 설정됨)

배포에 포함된 파일
- `Dockerfile`: 서버용 실행 설정(호스팅 대부분에서 바로 사용 가능)
- `wsgi.py`: gunicorn 실행 엔트리
- `.env.example`: 서버 환경변수 예시
- `render.yaml`: Render에서 원클릭에 가깝게 배포(디스크 포함)

### Render로 배포(추천)

빠른 시작(원클릭에 가깝게):
- `https://render.com/deploy?repo=https://github.com/ssungjun83/cutesoyeon`

1. Render 가입/로그인
2. Blueprint(또는 “New +”)로 이 GitHub 저장소 선택
3. `CHAT_APP_PASSWORD_HASH`만 입력 (생성: 로컬에서 `.\.venv\Scripts\python tools\hash_password.py`)
4. 배포 완료 후, Render에서 제공하는 URL로 접속 → 비밀번호 입력 → `가져오기`로 `.txt` 업로드

## 대화 추가 방법

- 웹 상단 `가져오기`에서 새 `.txt` 파일을 계속 업로드하면 누적 저장됩니다.
- 같은 파일을 여러 번 올려도 **중복은 자동으로 제외**됩니다.
- (선택) 가져오기 시 이름을 통일하려면 환경변수 `CHAT_APP_CANONICAL_ME_NAME` / `CHAT_APP_CANONICAL_OTHER_NAME`를 설정하세요. (가져오기 화면의 “기존 데이터 정리”로 과거 데이터도 일괄 적용 가능)

## 추억 사진 탭 (Google Drive 연동)

추억 탭은 **Google Drive 폴더를 사진 보관소로 사용**합니다.  
앱에서 업로드하면 Drive로 올라가고, Drive에 직접 올린 사진은 “Drive에서 불러오기”로 가져옵니다.

준비물
1. Google Cloud에서 **Drive API 활성화**
2. **서비스 계정** 생성 후 JSON 키 다운로드
3. Drive에서 사진 폴더를 만들고, **서비스 계정 이메일과 폴더 공유**
4. 환경변수 설정

필수 환경변수
- `CHAT_APP_DRIVE_FOLDER_ID`: 사진 폴더 ID
- `CHAT_APP_DRIVE_SERVICE_ACCOUNT_JSON` 또는 `CHAT_APP_DRIVE_SERVICE_ACCOUNT_B64`

메모
- Render에서는 서비스의 Environment에 위 변수를 추가하세요.
- 개인정보 보호를 위해 Drive 폴더는 “링크 공유” 대신 **서비스 계정만 공유**하는 방식을 권장합니다.

## GitHub 백업 (Render에서 자동 업로드)

Render에서 가져오기/정리를 할 때마다 DB를 텍스트로 내보내서 GitHub에 덮어쓰기 업로드할 수 있습니다.

필요한 환경변수
- `CHAT_APP_GITHUB_TOKEN`: GitHub PAT (repo 쓰기 권한 필요)
- `CHAT_APP_GITHUB_REPO`: `owner/repo` 형식 (예: `ssungjun83/cutesoyeon`)
- `CHAT_APP_GITHUB_BRANCH`: 기본 `main`
- `CHAT_APP_GITHUB_BACKUP_PREFIX`: 확장자 없는 prefix (예: `backup/chat_export`)

업로드되는 파일(매번 덮어쓰기)
- `backup/chat_export.txt` (단순 텍스트)
- `backup/chat_export_kakao.txt` (카카오 원문 스타일)
- `backup/chat_export.csv`

## 파일/데이터 위치

- DB: `data/chat.db` (로컬에만 저장)
- 비밀번호 설정: `.env` (`CHAT_APP_PASSWORD_HASH`)

## 외부(인터넷)에서 접속하려면 (선택)

1. `.env`에서 `CHAT_APP_HOST=0.0.0.0` 로 변경
2. 공유기/방화벽에서 `8000` 포트 열기(포트포워딩)
3. `http://내공인IP:8000` 로 접속 (비밀번호는 그대로 적용)

## 개발 메모(추가 개발할 때 참고)

### 목표/요구사항 정리
- 카카오톡 대화 `.txt`를 웹에 게시 (로그인 비밀번호 필요)
- 대화는 계속 추가(누적 Import)
- 중복 데이터는 제외: **이름이 달라도** “날짜/시간(분 단위) + 메시지 내용”이 같으면 중복으로 처리

### 구성(파일)
- `webapp.py`: Flask 앱(로그인/대화 보기/가져오기 업로드)
- `kakao_parser.py`: 카카오톡 txt 파서 (날짜 구분선 + `[이름] [오전/오후 시:분] ...` 형식)
- `storage.py`: SQLite 저장/조회 + 중복 제거 키 생성
- `templates/`: 화면(`login.html`, `chat.html`, `chat_txt.html`, `import.html`)
- `static/style.css`: 스타일
- `tools/set_password.py`: 로컬용 `.env` 생성(비밀번호 해시 저장)
- `tools/hash_password.py`: 서버 환경변수에 넣을 “비밀번호 해시” 출력
- `Dockerfile`/`wsgi.py`: 서버용(gunicorn) 실행
- `render.yaml`: Render 배포용(디스크 포함)

### 데이터/중복 제거 로직
- DB: `chat.db` (기본은 `data/chat.db`, 서버에서는 `CHAT_APP_DATA_DIR`로 변경 가능)
- 중복키(`dedup_key`): `dt_minute(YYYY-MM-DDTHH:MM)` + `norm_text(끝 공백 제거/개행 정리)`를 합쳐 `sha256`
- 실제 저장은 `INSERT OR IGNORE`로 처리해서 같은 키는 자동 스킵

### 파싱 규칙(현재 구현)
- 날짜 구분선 예: `--------------- 2025년 11월 9일 일요일 ---------------`
- 메시지 1줄 예: `[이름] [오전 9:25] 내용`
- 메시지 내용이 여러 줄이면 다음 메시지 헤더가 나오기 전까지 이전 메시지에 `\n`으로 이어붙임
- 업로드 파일 인코딩은 `utf-8-sig` → `utf-8` → `cp949` 순으로 시도

### 보안/운영 메모
- 비밀번호는 평문 저장하지 않고 `CHAT_APP_PASSWORD_HASH`(Werkzeug 해시)로만 검증
- `.env`는 GitHub에 올리지 않도록 `.gitignore`에 포함
- 개인정보 보호를 위해 `KakaoTalk_*.txt` 및 이미지 파일은 기본적으로 GitHub 업로드 제외(`.gitignore`)
- 서버 상시 운영 시 반드시 “영구 디스크/볼륨”을 붙여 DB가 유지되게 할 것(예: Render는 `render.yaml`에서 자동)
