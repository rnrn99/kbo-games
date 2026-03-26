"""KBO 공식 사이트 스케줄 API 클라이언트."""

import logging
import time

import requests

logger = logging.getLogger(__name__)

KBO_SCHEDULE_URL = "https://www.koreabaseball.com/ws/Schedule.asmx/GetScheduleList"

HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Referer": "https://www.koreabaseball.com/Schedule/Schedule.aspx",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.koreabaseball.com",
}

# srIdList 값
SR_ID_REGULAR = "0,9,6"       # 정규시즌
SR_ID_POSTSEASON = "3,4,5,7"  # 포스트시즌


def create_session() -> requests.Session:
    """쿠키를 공유하는 세션을 생성한다. KBO 메인 페이지를 먼저 방문해 세션 쿠키를 얻는다."""
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        logger.debug("KBO 메인 페이지 방문 중 (세션 쿠키 획득)...")
        resp = session.get(
            "https://www.koreabaseball.com/Schedule/Schedule.aspx",
            timeout=15,
        )
        resp.raise_for_status()
        logger.debug("세션 쿠키: %s", dict(session.cookies))
    except requests.RequestException as e:
        logger.warning("메인 페이지 방문 실패 (쿠키 없이 진행): %s", e)

    return session


def fetch_schedule(
    session: requests.Session,
    season: int,
    month: int,
    sr_id_list: str,
    retry: int = 3,
    delay: float = 1.0,
) -> dict:
    """지정한 시즌·월의 스케줄 데이터를 반환한다.

    Args:
        session:    공유 requests.Session
        season:     연도 (예: 2025)
        month:      월 (1~12)
        sr_id_list: "0,9,6" (정규) 또는 "3,4,5,7" (포스트)
        retry:      실패 시 재시도 횟수
        delay:      요청 간 대기 초

    Returns:
        API JSON 응답 dict. 파싱 실패 시 {"rows": []} 반환.
    """
    payload = {
        "leId": "1",
        "srIdList": sr_id_list,
        "seasonId": str(season),
        "gameMonth": f"{month:02d}",
        "teamId": "",
    }
    label = f"{season}-{month:02d} (srId={sr_id_list})"

    for attempt in range(1, retry + 1):
        try:
            logger.debug("[%s] 요청 중... (시도 %d/%d)", label, attempt, retry)
            resp = session.post(KBO_SCHEDULE_URL, data=payload, timeout=20)
            resp.raise_for_status()

            data = resp.json()
            row_count = len(data.get("rows", []))
            logger.info("[%s] 응답 수신 완료 — %d 경기", label, row_count)
            return data

        except requests.HTTPError as e:
            logger.warning("[%s] HTTP 오류: %s", label, e)
        except requests.RequestException as e:
            logger.warning("[%s] 요청 오류: %s", label, e)
        except ValueError as e:
            logger.warning("[%s] JSON 파싱 오류: %s", label, e)
            logger.debug("응답 본문: %s", resp.text[:500] if "resp" in dir() else "N/A")

        if attempt < retry:
            time.sleep(delay * attempt)

    logger.error("[%s] 모든 시도 실패 — 빈 데이터 반환", label)
    return {"rows": []}
