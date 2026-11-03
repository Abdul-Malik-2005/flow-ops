import asyncio

from sqlalchemy import text

from app.db.session import engine


async def main():
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT 1"))
        print(f"Database connection successful: {result.scalar_one()}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
