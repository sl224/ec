from sqlalchemy.orm import declarative_base, sessionmaker

# 1. The Single Source of Truth for all Models
Base = declarative_base()

# 2. Session Factory (Optional but recommended to keep here)
# You can configure the engine later using Session.configure(bind=eng)
SessionLocal = sessionmaker(autocommit=False, autoflush=False)
