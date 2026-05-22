#################################################################################
# models.py
#
# SQLAlchemy ORM models for tracking users, sessions, and events.
#
# Tables:
# - User: unique user (user_id), one-to-many with Session.
# - Session: UUID session_id, linked to User, optional browser/device info,
#            one-to-many with Event.
# - Event: linked to Session, stores timestamp and event type.

#################################################################################

import uuid

from db_utils.database import get_engine
from sqlalchemy import Integer, String, ForeignKey, BigInteger
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    sessions: Mapped[list["Session"]] = relationship(back_populates="user")


class Session(Base):
    __tablename__ = "sessions"
    session_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.user_id"))
    browser: Mapped[str] = mapped_column(String(50), nullable=True)
    device: Mapped[str] = mapped_column(String(50), nullable=True)
    events: Mapped[list["Event"]] = relationship(back_populates="session")
    user: Mapped["User"] = relationship(back_populates="sessions")


class Event(Base):
    __tablename__ = "events"
    event_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("sessions.session_id"))
    timestamp: Mapped[int] = mapped_column(BigInteger)
    type: Mapped[str] = mapped_column(String(50))
    session: Mapped["Session"] = relationship(back_populates="events")


engine = get_engine()


def initialize_database() -> None:
    """Create application tables used by the local demo stack."""
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    initialize_database()
