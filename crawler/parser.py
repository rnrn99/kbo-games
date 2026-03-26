"""KBO API 응답(HTML 테이블 형식 JSON)을 games 테이블 레코드로 변환한다."""

import logging
import re
from datetime import date
from typing import Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# game_id 형식: YYYYMMDD + away_code(2자) + home_code(2자) + dh_flag(1자)
_GAME_ID_RE = re.compile(r"^(\d{8})([A-Z]{2})([A-Z]{2})(\d)$")

# relay/highlight 셀 URL에서 gameId 추출
_GAME_ID_URL_RE = re.compile(r"gameId=([^&'\"]+)")

# 날짜 텍스트: '03.22(토)' → month, day
_DATE_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})")

# 팀명(play 셀 span 텍스트) → game_id 코드
TEAM_NAME_TO_CODE: dict[str, str] = {
    "KIA":  "HT",
    "LG":   "LG",
    "KT":   "KT",
    "SSG":  "SK",
    "두산": "OB",
    "롯데": "LT",
    "삼성": "SS",
    "한화": "HH",
    "NC":   "NC",
    "키움": "WO",
}


# ── 셀별 파싱 헬퍼 ────────────────────────────────────────────────────────────

def _extract_game_id(text: Optional[str]) -> Optional[str]:
    """relay 셀 HTML에서 gameId 파라미터를 추출한다."""
    if not text:
        return None
    m = _GAME_ID_URL_RE.search(text)
    return m.group(1) if m else None


def _extract_team_names_from_play(html: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """play 셀 HTML에서 (away_name, home_name)을 추출한다.

    구조: <span>팀A</span><em>...</em><span>팀B</span>
    """
    if not html:
        return None, None
    soup = BeautifulSoup(html, "html.parser")
    top_spans = [tag for tag in soup.children if getattr(tag, "name", None) == "span"]
    if len(top_spans) >= 2:
        return top_spans[0].get_text(strip=True), top_spans[-1].get_text(strip=True)
    return None, None


def _parse_date_text(text: str, season: int) -> Optional[date]:
    """'03.22(토)' 형식의 날짜 텍스트를 date 객체로 변환한다."""
    m = _DATE_RE.match(text.strip())
    if not m:
        return None
    try:
        return date(season, int(m.group(1)), int(m.group(2)))
    except ValueError:
        logger.warning("날짜 변환 실패: %s (season=%d)", text, season)
        return None


def _parse_play_cell(
    html: Optional[str],
) -> tuple[Optional[int], Optional[int], str, Optional[str]]:
    """play 셀 HTML을 파싱해 (away_score, home_score, status, cancel_reason)을 반환한다.

    HTML 예시:
        완료: <span>팀A</span><em><span class="lose">2</span><span>vs</span>
                <span class="win">5</span></em><span>팀B</span>
        예정/취소: <span>팀A</span><em><span>vs</span></em><span>팀B</span>
    """
    if not html:
        return None, None, "scheduled", None

    soup = BeautifulSoup(html, "html.parser")
    em = soup.find("em")
    if not em:
        return None, None, "scheduled", None

    # 점수 추출: em 안의 win/lose/same 클래스 span (same = 무승부)
    score_spans = em.find_all("span", class_=lambda c: c in ("win", "lose", "same"))
    if len(score_spans) == 2:
        try:
            away_score = int(score_spans[0].get_text(strip=True))
            home_score = int(score_spans[1].get_text(strip=True))
            return away_score, home_score, "completed", None
        except ValueError:
            pass

    return None, None, "scheduled", None


def _resolve_result(
    home_score: Optional[int], away_score: Optional[int], status: str
) -> Optional[str]:
    """홈팀 기준 win / lose / draw 또는 None."""
    if status != "completed" or home_score is None or away_score is None:
        return None
    if home_score > away_score:
        return "win"
    if home_score < away_score:
        return "lose"
    return "draw"


# ── 행 파싱 ──────────────────────────────────────────────────────────────────

def _get_cell(cells: list, cls: str) -> Optional[dict]:
    """Class 속성으로 셀을 찾는다."""
    return next((c for c in cells if c.get("Class") == cls), None)


def _parse_row(
    cells: list, current_date: date, game_type: str, season: int
) -> Optional[dict]:
    """셀 리스트 하나를 게임 레코드로 변환한다. 필수 정보 누락 시 None 반환."""
    play_cell = _get_cell(cells, "play")
    relay_cell = _get_cell(cells, "relay")

    if not play_cell:
        return None

    relay_text = relay_cell["Text"] if relay_cell else None
    game_id = _extract_game_id(relay_text)
    from_relay = game_id is not None

    if from_relay:
        m = _GAME_ID_RE.match(game_id)
        if not m:
            logger.warning("game_id 형식 불일치: %s", game_id)
            return None
        away_code = m.group(2)
        home_code = m.group(3)
        dh_flag = int(m.group(4))
    else:
        # relay 없음 → 팀명으로 코드 도출
        away_name, home_name = _extract_team_names_from_play(play_cell["Text"])
        if not away_name or not home_name:
            return None
        away_code = TEAM_NAME_TO_CODE.get(away_name)
        home_code = TEAM_NAME_TO_CODE.get(home_name)
        if not away_code or not home_code:
            logger.warning("알 수 없는 팀명: away=%s home=%s", away_name, home_name)
            return None
        dh_flag = 0
        game_id = f"{current_date.strftime('%Y%m%d')}{away_code}{home_code}{dh_flag}"

    away_score, home_score, status, cancel_reason = _parse_play_cell(play_cell["Text"])

    # relay 없을 때 마지막 셀로 취소 감지
    if not from_relay:
        last_text = cells[-1].get("Text", "").strip() if cells else ""
        if last_text and last_text != "-":
            status = "canceled"
            cancel_reason = last_text

    result_home = _resolve_result(home_score, away_score, status)

    # 경기장: 항상 마지막에서 두 번째 셀
    stadium_text = cells[-2]["Text"] if len(cells) >= 2 else None
    stadium = stadium_text if stadium_text and stadium_text not in ("-", "") else None

    return {
        "game_id":       game_id,
        "game_date":     current_date.isoformat(),
        "home_code":     home_code,
        "away_code":     away_code,
        "home_score":    home_score,
        "away_score":    away_score,
        "result_home":   result_home,
        "status":        status,
        "cancel_reason": cancel_reason,
        "game_type":     game_type,
        "double_header": dh_flag,
        "stadium":       stadium,
        "season":        season,
    }


# ── 공개 API ──────────────────────────────────────────────────────────────────

def parse_games(raw_data: dict, game_type: str, season: int) -> list[dict]:
    """API 응답 하나를 게임 레코드 리스트로 변환한다.

    Args:
        raw_data:  fetch_schedule()가 반환한 dict
        game_type: "regular" 또는 "postseason"
        season:    연도 (예: 2025)

    Returns:
        games 테이블에 upsert 가능한 dict 리스트.
    """
    rows = raw_data.get("rows", [])
    if not rows:
        logger.debug("rows 없음 (game_type=%s)", game_type)
        return []

    results: list[dict] = []
    seen_ids: set[str] = set()
    current_date: Optional[date] = None

    for i, row_obj in enumerate(rows):
        try:
            cells = row_obj.get("row", [])
            if not cells:
                continue

            # 날짜 셀이 있으면 current_date 갱신 (RowSpan으로 공유되는 셀)
            date_cell = _get_cell(cells, "day")
            if date_cell:
                new_date = _parse_date_text(date_cell["Text"], season)
                if new_date:
                    current_date = new_date

            if current_date is None:
                logger.debug("날짜 미확정, 행 %d 건너뜀", i)
                continue

            game = _parse_row(cells, current_date, game_type, season)
            if game is None:
                continue

            gid = game["game_id"]
            if gid in seen_ids:
                logger.debug("중복 game_id 건너뜀: %s", gid)
                continue
            seen_ids.add(gid)
            results.append(game)

        except Exception as e:
            logger.warning("행 %d 파싱 오류: %s", i, e)

    logger.info("파싱 완료: %d / %d 행 성공", len(results), len(rows))
    return results


def merge_games(regular: list[dict], postseason: list[dict]) -> list[dict]:
    """정규시즌·포스트시즌 리스트를 합친다. game_id 중복 시 postseason 우선."""
    merged: dict[str, dict] = {g["game_id"]: g for g in regular}
    for g in postseason:
        merged[g["game_id"]] = g
    return list(merged.values())
