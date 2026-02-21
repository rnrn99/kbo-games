"""Supabase 연결 및 games 테이블 upsert 헬퍼."""

import logging
import os
from typing import Optional

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

logger = logging.getLogger(__name__)

# 팀 코드(game_id_code) → teams.id 캐시
_team_id_cache: dict[str, int] = {}


def get_client() -> Client:
    """환경변수에서 Supabase 클라이언트를 생성한다."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_API_SECRET_KEY")
    if not url or not key:
        raise EnvironmentError(
            "SUPABASE_URL 과 SUPABASE_API_SECRET_KEY 환경변수를 설정하세요."
        )
    return create_client(url, key)


def load_team_map(client: Client) -> dict[str, int]:
    """teams 테이블을 읽어 {game_id_code: id} 딕셔너리를 반환한다.
    결과는 모듈 수준 캐시에 저장되므로 재요청하지 않는다.
    """
    global _team_id_cache
    if _team_id_cache:
        return _team_id_cache

    resp = client.table("teams").select("id, game_id_code").execute()
    rows = resp.data or []
    if not rows:
        raise RuntimeError("teams 테이블이 비어 있습니다. 먼저 시드 데이터를 삽입하세요.")

    _team_id_cache = {row["game_id_code"]: row["id"] for row in rows}
    logger.info("팀 맵 로드 완료: %s", _team_id_cache)
    return _team_id_cache


def resolve_team_ids(game: dict, team_map: dict[str, int]) -> Optional[dict]:
    """game 딕셔너리의 home_code / away_code 를 home_team_id / away_team_id 로 교체한다.
    어느 한 쪽이라도 팀 맵에 없으면 None 을 반환하고 경고 로그를 남긴다.
    """
    home_code = game.get("home_code")
    away_code = game.get("away_code")

    home_id = team_map.get(home_code)
    away_id = team_map.get(away_code)

    if home_id is None:
        logger.warning("알 수 없는 홈팀 코드: '%s' (game_id=%s)", home_code, game.get("game_id"))
        return None
    if away_id is None:
        logger.warning("알 수 없는 어웨이팀 코드: '%s' (game_id=%s)", away_code, game.get("game_id"))
        return None

    record = {k: v for k, v in game.items() if k not in ("home_code", "away_code")}
    record["home_team_id"] = home_id
    record["away_team_id"] = away_id
    return record


def upsert_games(client: Client, games: list[dict], team_map: dict[str, int]) -> tuple[int, int]:
    """게임 레코드를 games 테이블에 upsert 한다.

    Args:
        client:   Supabase 클라이언트
        games:    parser.parse_games() 가 반환한 레코드 리스트
        team_map: load_team_map() 가 반환한 dict

    Returns:
        (success_count, skip_count) 튜플
    """
    if not games:
        return 0, 0

    records = []
    skip = 0
    for game in games:
        record = resolve_team_ids(game, team_map)
        if record is None:
            skip += 1
            continue
        records.append(record)

    if not records:
        logger.warning("upsert 할 레코드 없음 (전부 팀 매핑 실패)")
        return 0, skip

    # 배치 크기: Supabase 단일 요청 권장 상한 ~500행
    BATCH = 500
    success = 0
    for i in range(0, len(records), BATCH):
        batch = records[i : i + BATCH]
        try:
            resp = (
                client.table("games")
                .upsert(batch, on_conflict="game_id")
                .execute()
            )
            inserted = len(resp.data) if resp.data else 0
            success += inserted
            logger.info(
                "upsert 배치 %d~%d: %d 행 처리",
                i + 1, i + len(batch), inserted,
            )
        except Exception as e:
            logger.error("upsert 실패 (배치 %d~%d): %s", i + 1, i + len(batch), e)
            skip += len(batch)

    return success, skip
