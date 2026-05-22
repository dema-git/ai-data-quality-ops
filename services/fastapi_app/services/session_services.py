############################################################################
# session_services.py
#
# This file contains service functions for session and analytics routes.
# These functions connect to the database and get information about:
# - top landing pages
# - best selling products
# - A/B test results
# - user session activity
#
# All functions are used in API route handlers to process requests.
###############################################################################

from fastapi import APIRouter, HTTPException
from models import User, Session as SessionModel, Event, engine
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from fastapi import HTTPException
from typing import Dict, Any, List
from exceptions_logging.logger import AppLogger


log = AppLogger(component="session_services")


def get_top_landing_pages_service(limit: int = 10) -> Dict[str, Any]:
    """
    Get the most popular pages with their view counts and bounce rates.
    Bounce rate is calculated using scroll depth (less than 30% means bounce).
    Returns: Dictionary with page statistics including URL, views, bounces,
    and bounce rate
    """
    log.info("get_top_landing_pages started", limit=limit)

    sql = text(
        """
        SELECT
            page_url,
            COUNT(*) AS page_views,
            COUNT(*) FILTER (
                WHERE scroll_depth IS NULL OR scroll_depth < 30
            ) AS bounces
        FROM mart.gold_page_views
        GROUP BY page_url
        ORDER BY page_views DESC
        LIMIT :limit
        """
    )

    try:
        with Session(engine) as db:
            result = db.execute(sql, {"limit": limit}).mappings().all()

        pages: List[Dict[str, Any]] = []
        for row in result:
            page_views = row["page_views"] or 0
            bounces = row["bounces"] or 0
            bounce_rate = bounces / page_views if page_views > 0 else 0.0

            pages.append(
                {
                    "page_url": row["page_url"],
                    "page_views": page_views,
                    "bounces": bounces,
                    "bounce_rate": bounce_rate,
                }
            )

        log.info("get_top_landing_pages done", groups_count=len(pages))
        return {
            "data": {
                "landing_pages": pages,
                "meta": {
                    "limit": limit,
                    "count": len(pages),
                },
            }
        }

    except SQLAlchemyError as e:
        log.exception(f"get_top_landing_pages database error: {e.args}", limit=limit)
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        log.exception(f"get_top_landing_pages unexpected error: {e.args}", limit=limit)
        raise HTTPException(status_code=500, detail="Internal server error")


def get_top_products_by_revenue_service(limit: int = 10) -> Dict[str, Any]:
    """
    Get the best selling products sorted by total revenue.
    Returns: Dictionary with product IDs, event counts,
    and total revenue for each product
    """
    log.info("get_top_products_by_revenue started", limit=limit)

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT
                        product_id,
                        COUNT(*) AS events_count,
                        SUM(price) AS total_revenue
                    FROM mart.gold_product_events
                    WHERE product_id IS NOT NULL AND product_id != 'NaN'
                    GROUP BY product_id
                    ORDER BY total_revenue DESC NULLS LAST
                    LIMIT :limit
                    """
                ),
                {"limit": limit},
            )

            rows = result.mappings().all()

        items: List[Dict[str, Any]] = []
        for r in rows:
            items.append(
                {
                    "product_id": r["product_id"],
                    "events_count": int(r["events_count"] or 0),
                    "total_revenue": float(r["total_revenue"] or 0),
                }
            )

        log.info("get_top_products_by_revenue done", groups_count=len(items))
        return {
            "data": {
                "items": items,
                "meta": {
                    "limit": limit,
                    "count": len(items),
                },
            }
        }

    except SQLAlchemyError as e:
        log.exception(f"get_top_products_by_revenue database error: {e.args}", limit=limit)
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        log.exception(f"get_top_products_by_revenue unexpected error: {e.args}", limit=limit)
        raise HTTPException(status_code=500, detail="Internal server error")


def get_ab_test_summary_service() -> Dict[str, Any]:
    """
    Get summary statistics for A/B test groups.
    Returns: Dictionary with statistics for each A/B test group.
    """
    log.info("get_ab_test_summary started")

    sql = text(
        """
        WITH page AS (
            SELECT
                ab_group,
                COUNT(*) AS page_views
            FROM mart.gold_page_views
            WHERE ab_group IS NOT NULL
            GROUP BY ab_group
        ),
        prod AS (
            SELECT
                ab_group,
                COUNT(DISTINCT session_id) AS sessions_with_product,
                COUNT(*) AS product_events
            FROM mart.gold_product_events
            WHERE ab_group IS NOT NULL
            GROUP BY ab_group
        )
        SELECT
            p.ab_group,
            p.page_views,
            COALESCE(pr.sessions_with_product, 0) AS sessions_with_product,
            COALESCE(pr.product_events, 0) AS product_events
        FROM page p
        LEFT JOIN prod pr ON p.ab_group = pr.ab_group
        ORDER BY p.ab_group
        """
    )

    try:
        with Session(engine) as db:
            rows = db.execute(sql).mappings().all()

        groups: List[Dict[str, Any]] = []
        for row in rows:
            groups.append(
                {
                    "ab_group": row["ab_group"],
                    "page_views": int(row["page_views"] or 0),
                    "sessions_with_product": int(row["sessions_with_product"] or 0),
                    "product_events": int(row["product_events"] or 0),
                }
            )

        log.info("get_ab_test_summary done", groups_count=len(groups))

        return {
            "data": {
                "groups": groups,
                "meta": {
                    "groups_count": len(groups),
                },
            }
        }

    except SQLAlchemyError as e:
        log.exception(f"get_ab_test_summary database error: {e.args}")
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        log.exception(f"get_ab_test_summary unexpected error: {e.args}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e.args}")


def get_user_sessions_overview_service(user_id: str, skip: int = 0,
                                       limit: int = 20, ) -> Dict[str, Any]:
    """
    Get overview of all sessions for a specific user.
    Returns:   Dictionary with session information and pagination details
    """

    log.info("get_user_sessions_overview started", user_id=user_id, skip=skip, limit=limit)

    # main query with pagination
    sql_sessions = text(
        """
        SELECT
            session_id,
            MIN(event_time) AS first_event_time,
            MAX(event_time) AS last_event_time,
            COUNT(*) AS page_views
        FROM mart.gold_page_views
        WHERE user_id = :user_id
        GROUP BY session_id
        ORDER BY first_event_time DESC
        LIMIT :limit OFFSET :offset
        """
    )

    # separate query to count total sessions
    sql_total = text(
        """
        SELECT COUNT(*) AS total_sessions
        FROM (
            SELECT DISTINCT session_id
            FROM mart.gold_page_views
            WHERE user_id = :user_id
        ) AS t
        """
    )

    try:
        with Session(engine) as db:
            total_row = db.execute(sql_total, {"user_id": user_id}).mappings().one()
            total_sessions = int(total_row["total_sessions"] or 0)

            result = db.execute(
                sql_sessions,
                {"user_id": user_id, "limit": limit, "offset": skip},
            ).mappings().all()

        sessions_data: List[Dict[str, Any]] = []
        for row in result:
            sessions_data.append(
                {
                    "session_id": row["session_id"],
                    "first_event_time": row["first_event_time"],
                    "last_event_time": row["last_event_time"],
                    "page_views": int(row["page_views"] or 0),
                }
            )

        log.info("get_user_sessions_overview done", groups_count=len(sessions_data))
        return {
            "data": {
                "sessions": sessions_data,
                "meta": {
                    "user_id": user_id,
                    "total_sessions": total_sessions,
                    "limit": limit,
                    "offset": skip,
                },
            }
        }

    except SQLAlchemyError as e:
        log.exception(
            f"get_user_sessions_overview database error: {e.args}",
            user_id=user_id,
            skip=skip,
            limit=limit,
        )
        raise HTTPException(status_code=500, detail="Database error")
    except Exception as e:
        log.exception(
            f"get_user_sessions_overview unexpected error: {e.args}",
            user_id=user_id,
            skip=skip,
            limit=limit,
        )
        raise HTTPException(status_code=500, detail=f"Internal server error: {e.args}")

