############################################################################
# crud.py
#
# Database helper functions for batch processing of Users, Sessions, and Events.
# Provides utilities to simplify creating or fetching model instances
# and adding events to the DB.
############################################################################

from models import User, Session, Event
from uuid import UUID
from exceptions_logging.logger import AppLogger


log = AppLogger(component="crud")

def get_or_create_user(db,
                       user_id: int,
                       users_cache: dict
                       ) -> User:
    """
    Returns a User instance from cache or database.
    The user is identified by a unique user_id generated from a LLaMA session.
    Lookup and creation are based solely on this session-derived ID.
    If the user does not exist, a new User is created, added to the database,
    and cached for faster access next time.
    """
    try:
        if user_id in users_cache:
            return users_cache[user_id]

        user = db.get(User, user_id)
        if not user:
            log.info("creating new user", user_id=user_id)
            user = User(user_id=user_id)
            db.add(user)

        users_cache[user_id] = user
        return user
    except Exception as e:
        log.exception(
            "failed to get_or_create_user",
            user_id=user_id
        )
        raise


def get_or_create_session(db,
                          session_id: UUID,
                          sessions_cache: dict,
                          user: User, browser: str | None,
                          device: str | None) -> Session:
    """
    Return a Session instance from the in-memory cache or the database.

    The session is identified by a unique session_id. Lookup and creation
    are based solely on this identifier. If a Session with the given
    session_id does not exist, a new one is created with the provided
    user, browser, and device information, added to the database session,
    and cached for subsequent access.
    """
    try:
        if session_id in sessions_cache:
            return sessions_cache[session_id]
        else:
            session = db.get(Session, session_id)
            if not session:
                log.info(
                    "creating new session",
                    session_id=str(session_id),
                    user_id=user.user_id,
                    browser=browser,
                    device=device
                )
                session = Session(
                    session_id=session_id,
                    user=user,
                    browser=browser,
                    device=device
                )
                db.add(session)
            sessions_cache[session_id] = session
            return session

    except Exception as e:
        log.exception(
            "failed to get_or_create_session",
            session_id=str(session_id),
            user_id=user.user_id
        )
        raise


def create_event(db, session:
                Session, timestamp,
                event_type: str) -> Event:
    """
    Create a new Event and add it to the database session.
    """
    try:
        if not event_type:
            log.warning(
                "creating event with empty event_type",
                session_id=str(session.session_id)
            )

        event = Event(
            session=session,
            timestamp=timestamp,
            type=event_type
        )
        db.add(event)
        return event

    except Exception:
        log.exception(
            "failed to create_event",
            session_id=str(session.session_id),
            event_type=event_type
        )
        raise