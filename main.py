"""KBO 스케줄 크롤러.

사용법:
    # 시즌 전체 적재 (3~11월)
    python main.py --season 2025

    # 오늘 날짜 기준 당월만 수집
    python main.py --today
"""

import argparse
import logging
import sys
import time
from datetime import date

from crawler.db import get_client, load_team_map, upsert_games
from crawler.kbo_api import SR_ID_POSTSEASON, SR_ID_REGULAR, create_session, fetch_schedule
from crawler.parser import merge_games, parse_games

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# 월별 수집 전략 — 매 시즌 동일 구조
MONTH_PLAN: dict[int, str] = {
    3:  "regular",
    4:  "regular",
    5:  "regular",
    6:  "regular",
    7:  "regular",
    8:  "regular",
    9:  "regular",
    10: "both",        # 정규 막판 + 포스트시즌 시작
    11: "postseason",
}

REQUEST_DELAY = 1.5  # 초 (요청 간 서버 부하 방지)


# ── 공통 수집 로직 ──────────────────────────────────────────────────────────────

def collect_month(session, season: int, month: int, plan: str) -> list[dict]:
    """한 달치 경기를 수집·파싱해 반환한다."""
    games: list[dict] = []

    if plan in ("regular", "both"):
        raw = fetch_schedule(session, season, month, SR_ID_REGULAR)
        games.extend(parse_games(raw, "regular", season))
        if plan == "both":
            time.sleep(REQUEST_DELAY)

    if plan in ("postseason", "both"):
        raw = fetch_schedule(session, season, month, SR_ID_POSTSEASON)
        postseason = parse_games(raw, "postseason", season)
        games = merge_games(games, postseason) if plan == "both" else games + postseason

    return games


def save(games: list[dict]) -> int:
    """게임 리스트를 Supabase 에 upsert 한다. 성공 시 0, 실패 시 1 반환."""
    if not games:
        logger.warning("저장할 경기가 없습니다.")
        return 1

    client = get_client()
    team_map = load_team_map(client)
    success, skip = upsert_games(client, games, team_map)
    logger.info("저장 완료 — 성공: %d, 건너뜀: %d", success, skip)
    return 0


# ── 모드별 진입점 ───────────────────────────────────────────────────────────────

def run_season(season: int) -> int:
    """연도별 전체 적재 — 시즌 전 월(3~11월)을 순서대로 수집한다."""
    logger.info("=== %d 시즌 전체 적재 시작 ===", season)
    session = create_session()
    all_games: list[dict] = []

    for month, plan in MONTH_PLAN.items():
        logger.info("── %d년 %d월 수집 (plan=%s) ──", season, month, plan)
        monthly = collect_month(session, season, month, plan)
        logger.info("%d월: %d 경기", month, len(monthly))
        all_games.extend(monthly)
        time.sleep(REQUEST_DELAY)

    logger.info("전체 수집 완료: %d 경기", len(all_games))
    return save(all_games)


def run_today() -> int:
    """일별 적재 — 오늘 날짜의 연도·월을 자동으로 사용한다."""
    today = date.today()
    season = today.year
    month = today.month

    if month not in MONTH_PLAN:
        logger.info("오늘(%s)은 KBO 시즌 외 기간입니다 (대상 월: 3~11월). 종료.", today)
        return 0

    plan = MONTH_PLAN[month]
    logger.info("=== 일별 적재: %d년 %d월 (plan=%s) ===", season, month, plan)

    session = create_session()
    games = collect_month(session, season, month, plan)
    logger.info("수집 완료: %d 경기", len(games))
    return save(games)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="KBO 경기 결과 크롤러")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--season", type=int, metavar="YEAR",
        help="시즌 전체 적재 (3~11월). 예: --season 2025",
    )
    group.add_argument(
        "--today", action="store_true",
        help="오늘 날짜 기준 당월만 수집",
    )
    args = parser.parse_args()

    if args.today:
        return run_today()
    return run_season(args.season)


if __name__ == "__main__":
    sys.exit(main())
