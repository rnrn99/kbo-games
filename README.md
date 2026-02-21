# KBO Games

KBO 공식 사이트에서 경기 일정과 결과를 수집하고 PostgreSQL DB에 저장합니다.

## 기술 스택

- **Python** 3.9+
- **Supabase** (PostgreSQL)

## 구조

```
kbo-games/
├── main.py
├── crawler/
│   ├── __init__.py
│   ├── kbo_api.py           # KBO API 호출 클라이언트
│   ├── parser.py            # API 응답 → DB 레코드 변환
│   └── db.py                # Supabase 연결 및 저장
├── requirements.txt
└── .env.example
```

### 각 파일 역할

**main.py**

두 가지 실행 모드를 CLI 인자로 선택합니다.

```bash
python main.py --season 2025   # 시즌 전체 적재 (3~11월)
python main.py --today         # 오늘 날짜 기준 당월만 수집
```

월별 수집 전략(`MONTH_PLAN`):

- 3~9월: 정규시즌만
- 10월: 정규 + 포스트시즌 모두
- 11월: 포스트시즌만

요청 간 1.5초 딜레이(`REQUEST_DELAY`)로 서버 부하를 방지합니다.

**crawler/kbo_api.py** — API 클라이언트

- 엔드포인트: `https://www.koreabaseball.com/ws/Schedule.asmx/GetScheduleList`
- `create_session()`: KBO 메인 페이지를 먼저 방문해 세션 쿠키를 획득한 뒤 공유 세션 생성
- `fetch_schedule()`: 시즌 + 월 + 종류(정규/포스트) 조합으로 POST 요청, 실패 시 최대 3회 재시도

**crawler/parser.py** — 파싱

KBO API는 HTML 테이블을 JSON으로 변환한 형태로 응답합니다. 각 row는 셀(`Class` 속성으로 역할 구분) 배열로 구성됩니다.

- `parse_games()`: API 응답 rows → DB 레코드 리스트 변환
  - 날짜: `Class="day"` 셀에서 추출, `RowSpan`으로 여러 행에 걸쳐 공유됨
  - 팀/점수: `Class="play"` 셀 HTML을 BeautifulSoup으로 파싱 (`win` / `lose` / `same` 클래스)
  - game_id: `Class="relay"` 셀 URL에서 추출 (`YYYYMMDD{away}{home}{dh}` 형식)
  - 팀 코드: game_id에서 직접 추출 (away 2자 + home 2자)
  - 경기 상태: "경기종료", "우천취소" 등 한국어 → `completed` / `canceled` / `scheduled`
  - 결과: 홈팀 기준 `win` / `lose` / `draw`
  - 더블헤더: `0`(단일) / `1`(1차전) / `2`(2차전)
- `merge_games()`: 정규 + 포스트 데이터가 겹칠 때 병합 (`game_id` 중복 시 포스트시즌 우선)

**crawler/db.py** — Supabase 저장

- `get_client()`: 환경변수로 Supabase 클라이언트 생성
- `load_team_map()`: `teams` 테이블에서 `{game_id_code → id}` 매핑 로드 (모듈 캐시)
- `resolve_team_ids()`: 팀 코드 문자열을 DB의 `team_id` 정수로 교체
- `upsert_games()`: 500행 배치로 나눠 `games` 테이블에 upsert (`on_conflict="game_id"`)

### 데이터 흐름

```
KBO API (POST)
    ↓  kbo_api.fetch_schedule()
raw JSON (HTML 테이블 형식)
    ↓  parser.parse_games()
게임 dict 리스트 (home_code/away_code 문자열)
    ↓  db.resolve_team_ids()
DB 레코드 (home_team_id/away_team_id 정수)
    ↓  db.upsert_games()
Supabase games 테이블
```

---

## 시작하기

### 환경 변수 설정

```bash
cp .env.example .env
```

```bash
# .env
SUPABASE_URL=your_supabase_url
SUPABASE_API_SECRET_KEY=your_supabase_secret_key
```

Supabase 대시보드 **Project Settings → API Keys** 에서 `sb_secret_...` 형식의 Secret 키를 사용하세요.

### 의존성 설치

```bash
pip install -r requirements.txt
```

### DB 스키마 적용

Supabase SQL Editor에서 실행합니다.

```sql
CREATE TABLE teams (
  id            SERIAL PRIMARY KEY,
  name          VARCHAR(20) NOT NULL,
  short_name    VARCHAR(10) NOT NULL,
  game_id_code  VARCHAR(5)  NOT NULL
);

CREATE TABLE games (
  id             SERIAL PRIMARY KEY,
  game_id        VARCHAR(20) UNIQUE NOT NULL,
  game_date      DATE NOT NULL,
  home_team_id   INT REFERENCES teams(id),
  away_team_id   INT REFERENCES teams(id),
  home_score     INT,
  away_score     INT,
  result_home    VARCHAR(10),
  status         VARCHAR(20) NOT NULL,
  cancel_reason  VARCHAR(30),
  game_type      VARCHAR(20) NOT NULL,
  double_header  INT DEFAULT 0,
  stadium        VARCHAR(20),
  season         INT NOT NULL,
  created_at     TIMESTAMP DEFAULT NOW(),
  updated_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_games_date      ON games(game_date);
CREATE INDEX idx_games_season    ON games(season);
CREATE INDEX idx_games_home_team ON games(home_team_id);
CREATE INDEX idx_games_away_team ON games(away_team_id);
```

## 라이선스

MIT
