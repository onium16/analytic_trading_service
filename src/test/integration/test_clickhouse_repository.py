# tests/test_clickhouse_repository.py

import pytest
import pytest_asyncio
from decimal import Decimal
from datetime import datetime, timezone
from pydantic import BaseModel
from infrastructure.storage.repositories.clickhouse_repository import ClickHouseRepository


class MyTestModel(BaseModel):
    id: int
    name: str
    price: Decimal
    ts: datetime


@pytest_asyncio.fixture(scope="function")
async def repo() -> ClickHouseRepository:
    """
    Асинхронная фикстура: создаёт временную таблицу в ClickHouse и удаляет её после теста.
    """
    table_name = "test_table"
    db_name = "test_db"

    repository = ClickHouseRepository(schema=MyTestModel, table_name=table_name, db=db_name)

    await repository.create_database()
    await repository.ensure_table()

    yield repository

    await repository.drop_table()
    await repository.drop_database()
    await repository.close()


@pytest.mark.asyncio
async def test_save_and_get_all(repo: ClickHouseRepository):
    record = MyTestModel(
        id=1,
        name="Test Item",
        price=Decimal("19.99"),
        ts=datetime.now(tz=timezone.utc)
    )

    await repo.save(record)
    results = await repo.get_all()

    assert len(results) == 1
    assert results[0].id == 1
    assert results[0].name == "Test Item"
